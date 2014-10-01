/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl2

import scala.concurrent.duration._
import akka.stream.testkit.StreamTestKit
import akka.stream.testkit2.TwoStreamsSetup
import akka.stream.scaladsl2.FlowGraphImplicits._

class GraphMergeSpec extends TwoStreamsSetup {

  override type Outputs = Int
  val op = Merge[Int]
  override def operationUnderTestLeft = op
  override def operationUnderTestRight = op

  "merge" must {

    "work in the happy case" in {
      // Different input sizes (4 and 6)
      val faucet1 = Pipe((0 to 3).iterator)
      val faucet2 = Pipe((4 to 9).iterator)
      val faucet3 = Pipe(List.empty[Int].iterator)
      val probe = StreamTestKit.SubscriberProbe[Int]()

      FlowGraph { implicit b ⇒
        val m1 = Merge[Int]("m1")
        val m2 = Merge[Int]("m2")
        val m3 = Merge[Int]("m3")

        faucet1 ~> m1 ~> Pipe[Int].map(_ * 2) ~> m2 ~> Pipe[Int].map(_ / 2).map(_ + 1) ~> SubscriberDrain(probe)
        faucet2 ~> m1
        faucet3 ~> m2

      }.run()

      val subscription = probe.expectSubscription()

      var collected = Set.empty[Int]
      for (_ ← 1 to 10) {
        subscription.request(1)
        collected += probe.expectNext()
      }

      collected should be(Set(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      probe.expectComplete()
    }

    "work with n-way merge" in {
      val faucet1 = Pipe(List(1))
      val faucet2 = Pipe(List(2))
      val faucet3 = Pipe(List(3))
      val faucet4 = Pipe(List(4))
      val faucet5 = Pipe(List(5))
      val faucet6 = Pipe(List.empty[Int])

      val probe = StreamTestKit.SubscriberProbe[Int]()

      FlowGraph { implicit b ⇒
        val merge = Merge[Int]("merge")

        faucet1 ~> merge ~> Pipe[Int] ~> SubscriberDrain(probe)
        faucet2 ~> merge
        faucet3 ~> merge
        faucet4 ~> merge
        faucet5 ~> merge
        faucet6 ~> merge

      }.run()

      val subscription = probe.expectSubscription()

      var collected = Set.empty[Int]
      for (_ ← 1 to 5) {
        subscription.request(1)
        collected += probe.expectNext()
      }

      collected should be(Set(1, 2, 3, 4, 5))
      probe.expectComplete()
    }

    commonTests()

    "work with one immediately completed and one nonempty publisher" in {
      val subscriber1 = setup(completedPublisher, nonemptyPublisher((1 to 4).iterator))
      val subscription1 = subscriber1.expectSubscription()
      subscription1.request(4)
      subscriber1.expectNext(1)
      subscriber1.expectNext(2)
      subscriber1.expectNext(3)
      subscriber1.expectNext(4)
      subscriber1.expectComplete()

      val subscriber2 = setup(nonemptyPublisher((1 to 4).iterator), completedPublisher)
      val subscription2 = subscriber2.expectSubscription()
      subscription2.request(4)
      subscriber2.expectNext(1)
      subscriber2.expectNext(2)
      subscriber2.expectNext(3)
      subscriber2.expectNext(4)
      subscriber2.expectComplete()
    }

    "work with one delayed completed and one nonempty publisher" in {
      val subscriber1 = setup(soonToCompletePublisher, nonemptyPublisher((1 to 4).iterator))
      val subscription1 = subscriber1.expectSubscription()
      subscription1.request(4)
      subscriber1.expectNext(1)
      subscriber1.expectNext(2)
      subscriber1.expectNext(3)
      subscriber1.expectNext(4)
      subscriber1.expectComplete()

      val subscriber2 = setup(nonemptyPublisher((1 to 4).iterator), soonToCompletePublisher)
      val subscription2 = subscriber2.expectSubscription()
      subscription2.request(4)
      subscriber2.expectNext(1)
      subscriber2.expectNext(2)
      subscriber2.expectNext(3)
      subscriber2.expectNext(4)
      subscriber2.expectComplete()
    }

    "work with one immediately failed and one nonempty publisher" in {
      // This is nondeterministic, multiple scenarios can happen
      pending
    }

    "work with one delayed failed and one nonempty publisher" in {
      // This is nondeterministic, multiple scenarios can happen
      pending
    }

  }

}
