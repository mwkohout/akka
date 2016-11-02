package akka.contrib.cache

import akka.actor.{PoisonPill, Props, UntypedActor}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{ MemberLeft, MemberUp, CurrentClusterState, ClusterDomainEvent }
import akka.routing.{ ConsistentHashingGroup, ConsistentHashingPool }

import scala.collection.SortedSet

class ClusterCache(val vnodes: Int, val entity: Props, val shutdownMessage: ) extends UntypedActor {
  val cluster = Cluster(context.system)
  cluster.subscribe(self, classOf[ClusterDomainEvent])

  val container = context.system.actorOf(Props(classOf[ContainerActor], entity), "container")

  var poolConfig = ConsistentHashingGroup(Set(container.path.toStringWithAddress(cluster.selfAddress))).withVirtualNodesFactor(vnodes)

  var poolGroup = context.actorOf(poolConfig.props(), "pool")

  var clusterMembers = SortedSet(cluster.selfAddress)

  /*L
  The paths may contain protocol and address information for actors running on remote hosts.
  Remoting requires the akka-remote module to be included in the classpath

string format:
  akka.tcp://app@10.0.0.1:2552/user/workers/w1
    */

  override def onReceive(message: Any): Unit = {
    message match {
      case state: CurrentClusterState
        if state.members.filterNot(state.getUnreachable.contains(_)).map(_.address) != clusterMembers ⇒
        clusterMembers = state.members.filterNot(state.getUnreachable.contains(_)).map(_.address)
        val paths = clusterMembers.map(a ⇒ container.path.toStringWithAddress(a))
        poolConfig = ConsistentHashingGroup(paths.toList).withVirtualNodesFactor(vnodes)
        poolGroup = context.actorOf(poolConfig.props())

      case MemberUp(member) if clusterMembers + member.address != clusterMembers ⇒
        clusterMembers += member.address
        val paths = clusterMembers.map(a ⇒ container.path.toStringWithAddress(a))
        poolConfig = ConsistentHashingGroup(paths.toList).withVirtualNodesFactor(vnodes)
        poolGroup = context.actorOf(poolConfig.props())

      case MemberLeft(member) if clusterMembers - member.address != clusterMembers ⇒
        clusterMembers -= member.address
        val paths = clusterMembers.map(a ⇒ container.path.toStringWithAddress(a))
        poolConfig = ConsistentHashingGroup(paths.toList).withVirtualNodesFactor(vnodes)
        poolGroup = context.actorOf(poolConfig.props())

      case message:_ if message == shutdownMessage =>
        poolGroup.tell(PoisonPill, self)
        container.tell(PoisonPill, self)

      case m:_=>
        poolGroup.tell(m, sender())
    }

  }

  class ContainerActor(entity: Props) extends UntypedActor {
    override def onReceive(message: Any): Unit = {

    }
  }

}