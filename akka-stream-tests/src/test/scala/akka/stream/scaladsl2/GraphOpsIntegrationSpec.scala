package akka.stream.scaladsl2

import akka.stream.testkit.AkkaSpec
import akka.stream.{ OverflowStrategy, MaterializerSettings }
import akka.stream.testkit.{ StreamTestKit, AkkaSpec }
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.stream.scaladsl2.FlowGraphImplicits._
import akka.stream.testkit.StreamTestKit.SubscriberProbe

class GraphOpsIntegrationSpec extends AkkaSpec {

  val settings = MaterializerSettings(system)
    .withInputBuffer(initialSize = 2, maxSize = 16)
    .withFanOutBuffer(initialSize = 1, maxSize = 16)

  implicit val materializer = FlowMaterializer(settings)

  "FlowGraphs" must {

    "support broadcast - merge layouts" in {
      val resultFuture = FutureSink[Seq[Int]]

      val g = FlowGraph { implicit b ⇒
        val bcast = Broadcast[Int]("broadcast")
        val merge = Merge[Int]("merge")

        FlowFrom(List(1, 2, 3)) ~> bcast
        bcast ~> merge
        bcast ~> FlowFrom[Int].map(_ + 3) ~> merge
        merge ~> FlowFrom[Int].grouped(10) ~> resultFuture
      }.run()

      Await.result(g.getSinkFor(resultFuture), 3.seconds).sorted should be(List(1, 2, 3, 4, 5, 6))
    }

    "support wikipedia Topological_sorting 2" in {
      // see https://en.wikipedia.org/wiki/Topological_sorting#mediaviewer/File:Directed_acyclic_graph.png
      val resultFuture2 = FutureSink[Seq[Int]]
      val resultFuture9 = FutureSink[Seq[Int]]
      val resultFuture10 = FutureSink[Seq[Int]]

      val g = FlowGraph { implicit b ⇒
        val b3 = Broadcast[Int]("b3")
        val b7 = Broadcast[Int]("b7")
        val b11 = Broadcast[Int]("b11")
        val m8 = Merge[Int]("m8")
        val m9 = Merge[Int]("m9")
        val m10 = Merge[Int]("m10")
        val m11 = Merge[Int]("m11")
        val in3 = IterableSource(List(3))
        val in5 = IterableSource(List(5))
        val in7 = IterableSource(List(7))

        // First layer
        in7 ~> b7
        b7 ~> m11
        b7 ~> m8

        in5 ~> m11

        in3 ~> b3
        b3 ~> m8
        b3 ~> m10

        // Second layer
        m11 ~> b11
        b11 ~> FlowFrom[Int].grouped(1000) ~> resultFuture2 // Vertex 2 is omitted since it has only one in and out
        b11 ~> m9
        b11 ~> m10

        m8 ~> m9

        // Third layer
        m9 ~> FlowFrom[Int].grouped(1000) ~> resultFuture9
        m10 ~> FlowFrom[Int].grouped(1000) ~> resultFuture10

      }.run()

      Await.result(g.getSinkFor(resultFuture2), 3.seconds).sorted should be(List(5, 7))
      Await.result(g.getSinkFor(resultFuture9), 3.seconds).sorted should be(List(3, 5, 7, 7))
      Await.result(g.getSinkFor(resultFuture10), 3.seconds).sorted should be(List(3, 5, 7))

    }

    "be able to run plain flow" in {
      val p = FlowFrom(List(1, 2, 3)).toPublisher()
      val s = SubscriberProbe[Int]
      val flow = FlowFrom[Int].map(_ * 2)
      FlowGraph { implicit builder ⇒
        import FlowGraphImplicits._
        PublisherSource(p) ~> flow ~> SubscriberSink(s)
      }.run()
      val sub = s.expectSubscription()
      sub.request(10)
      s.expectNext(1 * 2)
      s.expectNext(2 * 2)
      s.expectNext(3 * 2)
      s.expectComplete()
    }

    "support continue transformation from undefined source/sink" in {
      val input1 = UndefinedSource[Int]
      val output1 = UndefinedSink[Int]
      val output2 = UndefinedSink[String]
      val partial = PartialFlowGraph { implicit builder ⇒
        val bcast = Broadcast[String]("bcast")
        input1 ~> FlowFrom[Int].map(_.toString) ~> bcast ~> FlowFrom[String].map(_.toInt) ~> output1
        bcast ~> FlowFrom[String].map("elem-" + _) ~> output2
      }

      val s1 = SubscriberProbe[Int]
      val s2 = SubscriberProbe[String]
      FlowGraph(partial) { builder ⇒
        builder.attachFlowWithSource(input1, FlowFrom(List(0, 1, 2).map(_ + 1)))
        builder.attachFlowWithSink(output1, FlowFrom[Int].filter(n ⇒ (n % 2) != 0).withSink(SubscriberSink(s1)))
        builder.attachFlowWithSink(output2, FlowFrom[String].map(_.toUpperCase).withSink(SubscriberSink(s2)))
      }.run()

      val sub1 = s1.expectSubscription()
      val sub2 = s2.expectSubscription()
      sub1.request(10)
      sub2.request(10)
      s1.expectNext(1)
      s1.expectNext(3)
      s1.expectComplete()
      s2.expectNext("ELEM-1")
      s2.expectNext("ELEM-2")
      s2.expectNext("ELEM-3")
      s2.expectComplete()
    }

  }

}
