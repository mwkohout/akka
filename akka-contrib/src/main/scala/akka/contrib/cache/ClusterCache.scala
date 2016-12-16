package akka.contrib.cache

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{ Cluster, UniqueAddress }

import akka.routing.{ ActorSelectionRoutee, ConsistentHashingRoutingLogic, Routee }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._

import scala.collection.SortedSet
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

class ClusterCache(val vnodes: Int, val entity: Props, val role: String,
                   extractEntityId: PartialFunction[Any, String]) extends UntypedActor {
  implicit val materializer = ActorMaterializer()

  val vNodeRange = Range(1, vnodes)
  val vNodesActors: Vector[ActorRef] = vNodeRange.map(index ⇒ context.system.actorOf(Props(classOf[LocalVNodeActor], entity, extractEntityId), index.toString)).toVector

  var clusterRing = vNodesActors.map { v ⇒ ActorSelectionRoutee(context.system.actorSelection(v.path)) }
  val clusterRingListener = context.actorOf(
    Props(
      classOf[ClusterRingListener],
      clusterRing,
      self, self.path / "clusterListener"), "clusterListener")
  val logic = ConsistentHashingRoutingLogic(context.system, vnodes, extractEntityId)

  var leavingMembers = Set[UniqueAddress]()

  override def onReceive(message: Any): Unit = {
    message match {
      case RingUpdate(ring) if ring != clusterRing ⇒
        clusterRing = ring
        handoffRebalance()
      case Shutdown() ⇒
        val shutdownSender = sender()
        val localRoutee = vNodesActors.map { v ⇒ ActorSelectionRoutee(context.system.actorSelection(v.path)) }
        clusterRing = clusterRing.filterNot(localRoutee.contains(_))
        handoffRebalance()
      case message: Any ⇒
        val entityId = extractEntityId(message)
        val routee = logic.select(entityId, clusterRing)
        routee.send(message, sender())

    }

  }

  def handoffRebalance(): Unit = {
    val source = Source(vNodesActors)
    val sink = Sink.foreachParallel(vNodesActors.size) { vnodeActor: ActorRef ⇒
      val lookup = { (entityId: String) ⇒ logic.select(entityId, clusterRing) }
      vnodeActor ! RebalanceVNode(nodeFinder = lookup)
    }(context.dispatcher)

    source.to(sink).run()
  }

  def recalcClusterActors(clusterMembers: SortedSet[UniqueAddress], vNodeIds: Range) = {
    clusterMembers.flatMap(address ⇒
      vNodeIds.map { vnodeId ⇒
        val vnodePath = self.path / vnodeId.toString
        ActorSelectionRoutee(context.actorSelection(vnodePath.toStringWithAddress(address.address)))
      }
    ).toVector
  }

}

private[cache] class ClusterRingListener(val local: Vector[ActorSelection], val subscriber: ActorRef, val peerPath: ActorPath) extends UntypedActor with ActorLogging {

  val cluster = Cluster(context.system)
  implicit val actorSelectionOrdering = Ordering.by[ActorSelection, String](as ⇒ as.pathString)

  var clusterInfo: Map[Address, Vector[ActorSelection]] = Map() + (cluster.selfAddress → local)

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    //#subscribe
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
    //#subscribe
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  def onReceive(message: Any) = message match {
    case MemberUp(member) ⇒
      log.info("Member is Up: {}", member.address)
      context.system.actorSelection(peerPath.toStringWithAddress(member.address)) ! ClusterInfo(vnodes = local, address = cluster.selfAddress)
    case UnreachableMember(member) if clusterInfo != (clusterInfo - member.address) ⇒
      clusterInfo = clusterInfo - member.address
      rebuildRingAndUpdateSubscriber
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) if clusterInfo != (clusterInfo - member.address) ⇒
      clusterInfo = clusterInfo - member.address
      rebuildRingAndUpdateSubscriber
      log.info(
        "Member is Removed: {} after {}",
        member.address, previousStatus)
    case _: MemberEvent ⇒ // ignore
    case ClusterInfo(vnodes, senderAddress) if clusterInfo != (clusterInfo + (senderAddress → vnodes)) ⇒
      clusterInfo = (clusterInfo + (senderAddress → vnodes))

      rebuildRingAndUpdateSubscriber
    case _ ⇒
      unhandled(message)
  }

  def rebuildRingAndUpdateSubscriber(): Unit = {
    val sortedVnodes: SortedSet[ActorSelection] = SortedSet(clusterInfo.values.flatten.toSeq: _*)
    subscriber ! RingUpdate(ring = sortedVnodes.map(ActorSelectionRoutee(_)).toVector)
  }

}

private[cache] case class ClusterInfo(vnodes: Vector[ActorSelection], address: Address)

private[cache] class LocalVNodeActor(val entity: Props, val extractEntityId: PartialFunction[Any, String]) extends UntypedActor with Stash {

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

//I am sent to myself when one of my expired data nodes has sucessfully terminated.
private[cache] case class EntityStopped(id: String)

//The ClusterCache receives this when there is an update to the cluster's ring
private[cache] case class RingUpdate(ring: Vector[ActorSelectionRoutee])

//I am sent from the ClusterCache actor to the LocalVNodeActor so it can figure out where entity actors should live in the cluster
private[cache] case class RebalanceVNode(nodeFinder: (String ⇒ Routee))

private[cache] case class Ack(host: Address)

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

case class Shutdown()
/**
 * This is sent when an actorsystem is shutting down/about to shut down.
 * @param address
 */
case class ShutdownNotification(address: UniqueAddress)