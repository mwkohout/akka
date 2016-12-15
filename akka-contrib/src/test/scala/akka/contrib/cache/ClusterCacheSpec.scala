package akka.contrib.cache

import akka.actor._
import akka.cluster.{ MemberStatus, Member, Cluster }
import akka.cluster.ClusterEvent._
import akka.routing.ActorSelectionRoutee
import akka.testkit.TestActors.EchoActor
import akka.testkit.{ TestProbe, TestActorRef, AkkaSpec }
import org.mockito.Mockito
import org.scalatest.GivenWhenThen
import org.scalatest.mockito.MockitoSugar
import org.scalatest.words.ShouldVerb
import scala.language.implicitConversions
import Mockito._

//todo: use ClusterSingletonRestart2Spec as a template
class ClusterCacheSpec extends AkkaSpec("""           //stolen from akka.cluster.ClusterSpec
    akka.cluster {
      auto-down-unreachable-after = 0s
      periodic-tasks-initial-delay = 120 seconds // turn off scheduled tasks
      publish-stats-interval = 0 s # always, when it happens
      failure-detector.implementation-class = akka.cluster.FailureDetectorPuppet
    }
    akka.actor.provider = "cluster"
    akka.remote.log-remote-lifecycle-events = off
    akka.remote.netty.tcp.port = 0
    #akka.loglevel = DEBUG
                                        """
) with MockitoSugar with GivenWhenThen with ShouldVerb {

  implicit val actorSelectionOrdering = Ordering.by[ActorSelection, String](as ⇒ as.pathString)

  "ClusterRingListener" should {

    "Notify new cluster members of my local attributes" in {

      val selfAddress = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress

      val cluster = Cluster(system)

      cluster.join(selfAddress)

      val subscriberProbe = TestProbe()
      val peerProbe = TestProbe()

      val localVnodes: Vector[ActorSelection] = Vector(system.actorSelection(testActor.path))
      val remotePath = cluster.remotePathOf(testActor)

      // private[cache] class ClusterRingListener(val local: Vector[ActorSelection],
      // val subscriber: ActorRef, val cluster: Cluster) extends UntypedActor with ActorLogging {

      val actorRef = TestActorRef[ClusterRingListener](Props(classOf[ClusterRingListener], localVnodes,
        subscriberProbe.ref, cluster, peerProbe.ref.path))

      val clusterCache = actorRef.underlyingActor

      peerProbe.expectMsg(ClusterInfo(vnodes = localVnodes, address = selfAddress))
    }

    "Notify my subscriber of cluster attribute changes" in {

      val selfAddress = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress

      val cluster = Cluster(system)

      cluster.join(selfAddress)

      val subscriberProbe = TestProbe()
      val peerProbe = TestProbe()

      val fakeRemoteProbe = TestProbe()

      val localVnodes: Vector[ActorSelection] = Vector(system.actorSelection(testActor.path))
      val fakeRemoteNodes: Vector[ActorSelection] = Vector(system.actorSelection(fakeRemoteProbe.ref.path))
      val allVnodes = localVnodes ++ fakeRemoteNodes

      val localSelections = localVnodes.map { actorSelection: ActorSelection ⇒ new ActorSelectionRoutee(actorSelection) }
      val allSelections = allVnodes.map { actorSelection: ActorSelection ⇒ new ActorSelectionRoutee(actorSelection) }
      val remotePath = cluster.remotePathOf(testActor)

      // private[cache] class ClusterRingListener(val local: Vector[ActorSelection],
      // val subscriber: ActorRef, val cluster: Cluster) extends UntypedActor with ActorLogging {

      val actorRef = TestActorRef[ClusterRingListener](Props(classOf[ClusterRingListener], localVnodes,
        subscriberProbe.ref, cluster, peerProbe.ref.path))

      val clusterCache = actorRef.underlyingActor

      assertResult(Map(selfAddress → localVnodes))(clusterCache.clusterInfo)

      actorRef.tell(ClusterInfo(vnodes = fakeRemoteNodes, address = Address("akka", "junk")), ActorRef.noSender) //new member telling us they exist!
      assertResult(RingUpdate(ring = (allSelections)))(subscriberProbe.expectMsgClass(classOf[RingUpdate]))

      actorRef.tell(ClusterInfo(vnodes = fakeRemoteNodes, address = Address("akka", "junk")), ActorRef.noSender) //this is a repeat
      subscriberProbe.expectNoMsg() //so we don't tell our subscriber
    }
  }

}
