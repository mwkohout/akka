package akka.contrib.cache

import akka.NotUsed
import akka.actor._
import akka.cluster.ClusterEvent.{ ClusterDomainEvent, CurrentClusterState, MemberLeft, MemberUp }
import akka.cluster.{ Cluster, UniqueAddress }
import akka.routing.{ ActorRefRoutee, ActorSelectionRoutee, ConsistentHashingRoutingLogic, Routee }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._

import scala.collection.SortedSet
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

class ClusterCache(val vnodes: Int, val entity: Props, val role: String,
                   extractEntityId:     PartialFunction[Any, String],
                   val shutdownMessage: Any) extends UntypedActor {
  implicit val materializer = ActorMaterializer()
  val cluster = Cluster(context.system)
  cluster.subscribe(self, classOf[ClusterDomainEvent])

  var clusterMembers = SortedSet(cluster.selfUniqueAddress)

  var clusterActors = recalcClusterActors(clusterMembers)

  val remoteLogic = ConsistentHashingRoutingLogic(context.system, vnodes, extractEntityId)
  val localLogic = ConsistentHashingRoutingLogic(context.system, vnodes, extractEntityId)

  val vNodesActors = Range(1, vnodes).map(index ⇒ ActorRefRoutee(context.system.actorOf(Props(classOf[LocalVNodeActor], entity, extractEntityId)))).toVector
  var leavingMembers = Set[UniqueAddress]()

  val meRoutee = ActorSelectionRoutee(context.actorSelection(self.path.toStringWithAddress(cluster.selfAddress)))

  override def onReceive(message: Any): Unit = {
    message match {
      case state: CurrentClusterState if state.members.filterNot(state.getUnreachable.contains(_)).map(_.address) != clusterMembers ⇒
        val members = state.members.filterNot(state.getUnreachable.contains(_) && state.allRoles.contains(role))
          .filter(
            member ⇒
              leavingMembers.find(_.address == member.address) match {
                case Some(oldMember) if oldMember < member.uniqueAddress ⇒ //it's a member on the blacklist and it has been restarted so it's good
                  true
                case Some(oldMember) if oldMember == member.uniqueAddress ⇒ //if it's a member on the blacklist and hasn't been restarted, it's not an active member
                  false
                case None ⇒ true //this node hasn't been marked as down so it's a valid member
              }
          ).map(_.uniqueAddress)
        leavingMembers = leavingMembers -- members //
        clusterMembers = members
        clusterActors = recalcClusterActors(clusterMembers)

      case MemberUp(member) if clusterMembers + member.uniqueAddress != clusterMembers && member.roles.contains(role) ⇒
        clusterMembers = clusterMembers + member.uniqueAddress
        clusterActors = recalcClusterActors(clusterMembers)

        leavingMembers.find(member.address == _.address) match {
          //if a node is restarted, remove it from the blacklist
          case Some(oldMember) if oldMember < member.uniqueAddress ⇒ leavingMembers -= oldMember
          case _ ⇒
        }

        //iterate through all my data members to see if they should hand off their data
        handoffRebalance()

      case MemberLeft(member) if clusterMembers - member.uniqueAddress != clusterMembers && member.roles.contains(role) ⇒
        clusterMembers = clusterMembers - member.uniqueAddress
        clusterActors = recalcClusterActors(clusterMembers)

        handoffRebalance()

        leavingMembers = leavingMembers.filter(_.address != member.address) //when they go, remove them from the blacklist

      //iterate through all my data members to see if they should hand off their data

      case MemberUp | MemberLeft ⇒ //no op.  this means the cluster looks like we expect

      case ShutdownNotification(member) ⇒ //if I receive this, I need to remove the sending system
        leavingMembers += member

        clusterMembers = clusterMembers - member
        clusterActors = recalcClusterActors(clusterMembers)
        handoffRebalance()
      case message: Any if message == shutdownMessage ⇒
        //if I receive this, I need to send everyone a ShutdownNotification and begin transferring my data to the new nodes.
        clusterActors.foreach(_.send(ShutdownNotification(address = cluster.selfUniqueAddress), self))

      case message: Any if message != shutdownMessage ⇒
        val entityId = extractEntityId(message)
        val remoteRoutee = remoteLogic.select(entityId, clusterActors)
        remoteRoutee == meRoutee match {
          case true ⇒
            val localRoutee = localLogic.select(entityId, vNodesActors)
            localRoutee.send(message, sender())
          case false ⇒ remoteRoutee.send(message, sender())
        }
    }

  }

  def handoffRebalance(): NotUsed = {
    val source = Source(vNodesActors)
    val sink = Sink.foreachParallel(vNodesActors.size) { vnodeActor: ActorRefRoutee ⇒
      val lookup = { (entityId: String) ⇒ remoteLogic.select(entityId, clusterActors) }
      vnodeActor.send(RebalanceVNode(nodeFinder = lookup), self)

    }(context.dispatcher)
    source.to(sink).run()
  }

  def recalcClusterActors(clusterMembers: SortedSet[UniqueAddress]) = {
    clusterMembers.map(address ⇒ ActorSelectionRoutee(context.actorSelection(self.path.toStringWithAddress(address.address)))).toVector
  }

  //I am sent to myself when one of my expired data nodes has sucessfully terminated.
  private case class EntityStopped(id: String)

  //I am sent from the ClusterCache actor to the LocalVNodeActor so it can figure out where entity actors should live in the cluster
  private case class RebalanceVNode(nodeFinder: (String ⇒ Routee))

  /**
   * I am sent to a data node during a rebalancing event.  I contain the destination of the data and my own name
   * @param newNode
   * @param key
   */
  case class Rebalance(newNode: Routee, key: String)

  /**
   * a data node can send this it it's owning LocalVNodeActor actor when it should be shut down.
   * @param key
   */
  case class Expire(key: String)

  /**
   * This is sent when an actorsystem is shutting down/about to shut down.
   * @param address
   */
  case class ShutdownNotification(address: UniqueAddress)

  class LocalVNodeActor(val entity: Props, val extractEntityId: PartialFunction[Any, String]) extends UntypedActor with Stash {

    val dataMembers = scala.collection.mutable.Map[String, ActorRef]()
    var expiringEntry = SortedSet[String]()

    override def onReceive(message: Any): Unit = message match {
      case EntityStopped(key) ⇒
        expiringEntry -= key
        unstashAll() //we could look for the messages to unstash, but is it worth it?

      case Expire(key) ⇒
        dataMembers.get(key) match {
          case Some(entityActor) ⇒
            expiringEntry += key
            dataMembers.remove(key)
            akka.pattern.gracefulStop(entityActor, 1.minute).andThen({
              //this is happening on a seperate thread, outside the actor
              case Success(_) ⇒ self.tell(EntityStopped(id = key), self)
              case Failure(_) ⇒ throw new Exception("couldn't shutdown expired data node") //I don't think we can recover from this.
            })(context.dispatcher)
          case None ⇒ //no op???
        }

      case _ ⇒
        val entityId = extractEntityId(message)
        expiringEntry.contains(entityId) match {
          case true ⇒ stash()
          case false ⇒ dataMembers.getOrElseUpdate(entityId, {
            context.system.actorOf(entity, entityId)
          }).tell(message, sender())
        }
    }
  }

}

