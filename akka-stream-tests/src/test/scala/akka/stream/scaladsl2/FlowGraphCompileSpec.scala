/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl2

import akka.stream.testkit.AkkaSpec
import akka.stream.Transformer
import akka.stream.OverflowStrategy
import akka.stream.testkit.StreamTestKit.SubscriberProbe
import akka.stream.testkit.StreamTestKit.PublisherProbe

object FlowGraphCompileSpec {
  class Fruit
  class Apple extends Fruit
}

class FlowGraphCompileSpec extends AkkaSpec {
  import FlowGraphCompileSpec._

  implicit val mat = FlowMaterializer()

  def op[In, Out]: () ⇒ Transformer[In, Out] = { () ⇒
    new Transformer[In, Out] {
      override def onNext(elem: In) = List(elem.asInstanceOf[Out])
    }
  }

  val f1 = Pipe[String].transform("f1", op[String, String])
  val f2 = Pipe[String].transform("f2", op[String, String])
  val f3 = Pipe[String].transform("f3", op[String, String])
  val f4 = Pipe[String].transform("f4", op[String, String])
  val f5 = Pipe[String].transform("f5", op[String, String])
  val f6 = Pipe[String].transform("f6", op[String, String])

  val in1 = IterableFaucet(List("a", "b", "c"))
  val in2 = IterableFaucet(List("d", "e", "f"))
  val out1 = PublisherDrain[String]
  val out2 = FutureDrain[String]

  "FlowGraph" should {
    "build simple merge" in {
      FlowGraph { b ⇒
        val merge = Merge[String]
        b.
          addEdge(in1, f1, merge).
          addEdge(in2, f2, merge).
          addEdge(merge, f3, out1)
      }.run()
    }

    "build simple broadcast" in {
      FlowGraph { b ⇒
        val bcast = Broadcast[String]
        b.
          addEdge(in1, f1, bcast).
          addEdge(bcast, f2, out1).
          addEdge(bcast, f3, out2)
      }.run()
    }

    "build simple merge - broadcast" in {
      FlowGraph { b ⇒
        val merge = Merge[String]
        val bcast = Broadcast[String]
        b.
          addEdge(in1, f1, merge).
          addEdge(in2, f2, merge).
          addEdge(merge, f3, bcast).
          addEdge(bcast, f4, out1).
          addEdge(bcast, f5, out2)
      }.run()
    }

    "build simple merge - broadcast with implicits" in {
      FlowGraph { implicit b ⇒
        import FlowGraphImplicits._
        val merge = Merge[String]
        val bcast = Broadcast[String]
        in1 ~> f1 ~> merge ~> f2 ~> bcast ~> f3 ~> out1
        in2 ~> f4 ~> merge
        bcast ~> f5 ~> out2
      }.run()
    }

    /**
     * in ---> f1 -+-> f2 -+-> f3 ---> out1
     *             ^       |
     *             |       V
     *             f5 <-+- f4
     *                  |
     *                  V
     *                  f6 ---> out2
     */
    "detect cycle in " in {
      intercept[IllegalArgumentException] {
        FlowGraph { b ⇒
          val merge = Merge[String]
          val bcast1 = Broadcast[String]
          val bcast2 = Broadcast[String]
          val feedbackLoopBuffer = Pipe[String].buffer(10, OverflowStrategy.dropBuffer)
          b.
            addEdge(in1, f1, merge).
            addEdge(merge, f2, bcast1).
            addEdge(bcast1, f3, out1).
            addEdge(bcast1, feedbackLoopBuffer, bcast2).
            addEdge(bcast2, f5, merge). // cycle
            addEdge(bcast2, f6, out2)
        }
      }.getMessage.toLowerCase should include("cycle")

    }

    "express complex topologies in a readable way" in {
      FlowGraph { implicit b ⇒
        b.allowCycles()
        val merge = Merge[String]
        val bcast1 = Broadcast[String]
        val bcast2 = Broadcast[String]
        val feedbackLoopBuffer = Pipe[String].buffer(10, OverflowStrategy.dropBuffer)
        import FlowGraphImplicits._
        in1 ~> f1 ~> merge ~> f2 ~> bcast1 ~> f3 ~> out1
        bcast1 ~> feedbackLoopBuffer ~> bcast2 ~> f5 ~> merge
        bcast2 ~> f6 ~> out2
      }.run()
    }

    "build broadcast - merge" in {
      FlowGraph { implicit b ⇒
        val bcast = Broadcast[String]
        val bcast2 = Broadcast[String]
        val merge = Merge[String]
        import FlowGraphImplicits._
        in1 ~> f1 ~> bcast ~> f2 ~> merge ~> f3 ~> out1
        bcast ~> f4 ~> merge
      }.run()
    }

    "build wikipedia Topological_sorting" in {
      // see https://en.wikipedia.org/wiki/Topological_sorting#mediaviewer/File:Directed_acyclic_graph.png
      FlowGraph { implicit b ⇒
        val b3 = Broadcast[String]
        val b7 = Broadcast[String]
        val b11 = Broadcast[String]
        val m8 = Merge[String]
        val m9 = Merge[String]
        val m10 = Merge[String]
        val m11 = Merge[String]
        val in3 = IterableFaucet(List("b"))
        val in5 = IterableFaucet(List("b"))
        val in7 = IterableFaucet(List("a"))
        val out2 = PublisherDrain[String]
        val out9 = PublisherDrain[String]
        val out10 = PublisherDrain[String]
        def f(s: String) = Pipe[String].transform(s, op[String, String])
        import FlowGraphImplicits._

        in7 ~> f("a") ~> b7 ~> f("b") ~> m11 ~> f("c") ~> b11 ~> f("d") ~> out2
        b11 ~> f("e") ~> m9 ~> f("f") ~> out9
        b7 ~> f("g") ~> m8 ~> f("h") ~> m9
        b11 ~> f("i") ~> m10 ~> f("j") ~> out10
        in5 ~> f("k") ~> m11
        in3 ~> f("l") ~> b3 ~> f("m") ~> m8
        b3 ~> f("n") ~> m10
      }.run()
    }

    "attachFaucet and attachDrain" in {
      val mg = FlowGraph { b ⇒
        val merge = Merge[String]
        val undefinedSrc1 = UndefinedFaucet[String]
        val undefinedSrc2 = UndefinedFaucet[String]
        val undefinedDrain1 = UndefinedDrain[String]
        b.
          addEdge(undefinedSrc1, f1, merge).
          addEdge(UndefinedFaucet[String]("src2"), f2, merge).
          addEdge(merge, f3, undefinedDrain1)

        b.attachFaucet(undefinedSrc1, in1)
        b.attachFaucet(UndefinedFaucet[String]("src2"), in2)
        b.attachDrain(undefinedDrain1, out1)

      }.run()
      out1.publisher(mg) should not be (null)
    }

    "build partial flow graphs" in {
      val undefinedSrc1 = UndefinedFaucet[String]
      val undefinedSrc2 = UndefinedFaucet[String]
      val undefinedDrain1 = UndefinedDrain[String]
      val bcast = Broadcast[String]

      val partial1 = PartialFlowGraph { implicit b ⇒
        import FlowGraphImplicits._
        val merge = Merge[String]
        undefinedSrc1 ~> f1 ~> merge ~> f2 ~> bcast ~> f3 ~> undefinedDrain1
        undefinedSrc2 ~> f4 ~> merge

      }
      partial1.undefinedFaucets should be(Set(undefinedSrc1, undefinedSrc2))
      partial1.undefinedDrains should be(Set(undefinedDrain1))

      val partial2 = PartialFlowGraph(partial1) { implicit b ⇒
        import FlowGraphImplicits._
        b.attachFaucet(undefinedSrc1, in1)
        b.attachFaucet(undefinedSrc2, in2)
        bcast ~> f5 ~> UndefinedDrain[String]("drain2")
      }
      partial2.undefinedFaucets should be(Set.empty)
      partial2.undefinedDrains should be(Set(undefinedDrain1, UndefinedDrain[String]("drain2")))

      FlowGraph(partial2) { implicit b ⇒
        b.attachDrain(undefinedDrain1, out1)
        b.attachDrain(UndefinedDrain[String]("drain2"), out2)
      }.run()
    }

    "make it optional to specify flows" in {
      FlowGraph { implicit b ⇒
        val merge = Merge[String]
        val bcast = Broadcast[String]
        import FlowGraphImplicits._
        in1 ~> merge ~> bcast ~> out1
        in2 ~> merge
        bcast ~> out2
      }.run()
    }

    "use SourcePipe and SinkPipe" in {
      FlowGraph { implicit b ⇒
        val bcast = Broadcast[String]
        import FlowGraphImplicits._
        f1.withFaucet(in1) ~> bcast ~> f2.withDrain(out1)
        bcast ~> f3.withDrain(out2)
      }.run()
    }

    "chain input and output ports" in {
      FlowGraph { implicit b ⇒
        val zip = Zip[Int, String]
        val out = PublisherDrain[(Int, String)]
        import FlowGraphImplicits._
        Pipe(List(1, 2, 3)) ~> zip.left ~> out
        Pipe(List("a", "b", "c")) ~> zip.right
      }.run()
    }

    "build unzip - zip" in {
      FlowGraph { implicit b ⇒
        val zip = Zip[Int, String]
        val unzip = Unzip[Int, String]
        val out = PublisherDrain[(Int, String)]
        import FlowGraphImplicits._
        Pipe(List(1 -> "a", 2 -> "b", 3 -> "c")) ~> unzip.in
        unzip.left ~> Pipe[Int].map(_ * 2) ~> zip.left
        unzip.right ~> zip.right
        zip.out ~> out
      }.run()
    }

    "distinguish between input and output ports" in {
      intercept[IllegalArgumentException] {
        FlowGraph { implicit b ⇒
          val zip = Zip[Int, String]
          val unzip = Unzip[Int, String]
          val wrongOut = PublisherDrain[(Int, Int)]
          val whatever = PublisherDrain[Any]
          import FlowGraphImplicits._
          "Pipe(List(1, 2, 3)) ~> zip.left ~> wrongOut" shouldNot compile
          """Pipe(List("a", "b", "c")) ~> zip.left""" shouldNot compile
          """Pipe(List("a", "b", "c")) ~> zip.out""" shouldNot compile
          "zip.left ~> zip.right" shouldNot compile
          "Pipe(List(1, 2, 3)) ~> zip.left ~> wrongOut" shouldNot compile
          """Pipe(List(1 -> "a", 2 -> "b", 3 -> "c")) ~> unzip.in ~> whatever""" shouldNot compile
        }
      }.getMessage should include("empty")
    }

    "check maximumInputCount" in {
      intercept[IllegalArgumentException] {
        FlowGraph { implicit b ⇒
          val bcast = Broadcast[String]
          import FlowGraphImplicits._
          in1 ~> bcast ~> out1
          in2 ~> bcast // wrong
        }
      }.getMessage should include("at most 1 incoming")
    }

    "check maximumOutputCount" in {
      intercept[IllegalArgumentException] {
        FlowGraph { implicit b ⇒
          val merge = Merge[String]
          import FlowGraphImplicits._
          in1 ~> merge ~> out1
          merge ~> out2 // wrong
        }
      }.getMessage should include("at most 1 outgoing")
    }

    "build with variance" in {
      val out = SubscriberDrain(SubscriberProbe[Fruit]())
      FlowGraph { b ⇒
        val merge = Merge[Fruit]
        b.
          addEdge(Pipe[Fruit](() ⇒ Some(new Apple)), merge).
          addEdge(Pipe[Apple](() ⇒ Some(new Apple)), merge).
          addEdge(merge, Pipe[Fruit].map(identity), out)
      }
    }

    "build with implicits and variance" in {
      PartialFlowGraph { implicit b ⇒
        val inA = PublisherFaucet(PublisherProbe[Fruit]())
        val inB = PublisherFaucet(PublisherProbe[Apple]())
        val outA = SubscriberDrain(SubscriberProbe[Fruit]())
        val outB = SubscriberDrain(SubscriberProbe[Fruit]())
        val merge = Merge[Fruit]
        import FlowGraphImplicits._
        Pipe[Fruit](() ⇒ Some(new Apple)) ~> merge
        Pipe[Apple](() ⇒ Some(new Apple)) ~> merge
        inA ~> merge
        inB ~> merge
        inA ~> Pipe[Fruit].map(identity) ~> merge
        inB ~> Pipe[Apple].map(identity) ~> merge
        UndefinedFaucet[Apple] ~> merge
        UndefinedFaucet[Apple] ~> Pipe[Fruit].map(identity) ~> merge
        UndefinedFaucet[Apple] ~> Pipe[Apple].map(identity) ~> merge
        merge ~> Pipe[Fruit].map(identity) ~> outA

        Pipe[Apple](() ⇒ Some(new Apple)) ~> Broadcast[Apple] ~> merge
        Pipe[Apple](() ⇒ Some(new Apple)) ~> Broadcast[Apple] ~> outB
        Pipe[Apple](() ⇒ Some(new Apple)) ~> Broadcast[Apple] ~> UndefinedDrain[Fruit]
        inB ~> Broadcast[Apple] ~> merge

        "UndefinedFaucet[Fruit] ~> Pipe[Apple].map(identity) ~> merge" shouldNot compile
        "UndefinedFaucet[Fruit] ~> Broadcast[Apple]" shouldNot compile
        "merge ~> Broadcast[Apple]" shouldNot compile
        "merge ~> Pipe[Fruit].map(identity) ~> Broadcast[Apple]" shouldNot compile
        "inB ~> merge ~> Broadcast[Apple]" shouldNot compile
        "inA ~> Broadcast[Apple]" shouldNot compile
      }
    }

  }
}
