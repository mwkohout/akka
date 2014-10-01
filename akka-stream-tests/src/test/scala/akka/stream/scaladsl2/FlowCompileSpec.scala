/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl2

import akka.stream.MaterializerSettings
import akka.stream.testkit.AkkaSpec
import scala.collection.immutable.Seq
import scala.concurrent.Future

class FlowCompileSpec extends AkkaSpec {

  val intSeq = IterableFaucet(Seq(1, 2, 3))
  val strSeq = IterableFaucet(Seq("a", "b", "c"))

  import scala.concurrent.ExecutionContext.Implicits.global
  val intFut = FutureFaucet(Future { 3 })
  implicit val materializer = FlowMaterializer(MaterializerSettings(system))

  "Flow" should {
    "should not run" in {
      val open: Flow[Int, Int] = Flow[Int]
      "open.run()" shouldNot compile
    }
    "accept Iterable" in {
      val f: Source[Int] = Flow[Int].prepend(intSeq)
    }
    "accept Future" in {
      val f: Source[Int] = Flow[Int].prepend(intFut)
    }
    "append Flow" in {
      val open1: Flow[Int, String] = Flow[Int].map(_.toString)
      val open2: Flow[String, Int] = Flow[String].map(_.hashCode)
      val open3: Flow[Int, Int] = open1.append(open2)
      "open3.run()" shouldNot compile

      val closedSource: Source[Int] = open3.prepend(intSeq)
      "closedSource.run()" shouldNot compile

      val closedSink: Sink[Int] = open3.append(PublisherDrain[Int])
      "closedSink.run()" shouldNot compile

      closedSource.append(PublisherDrain[Int]).run()
      closedSink.prepend(intSeq).run()
    }
    "prependFlow" in {
      val open1: Flow[Int, String] = Flow[Int].map(_.toString)
      val open2: Flow[String, Int] = Flow[String].map(_.hashCode)
      val open3: Flow[String, String] = open1.prepend(open2)
      "open3.run()" shouldNot compile

      val closedSource: Source[String] = open3.prepend(strSeq)
      "closedSource.run()" shouldNot compile

      val closedSink: Sink[String] = open3.append(PublisherDrain[String])
      "closedDrain.run()" shouldNot compile

      closedSource.append(PublisherDrain[String]).run
      closedSink.prepend(strSeq).run
    }

    "append Sink" in {
      val open: Flow[Int, String] = Flow[Int].map(_.toString)
      val closedDrain: Sink[String] = Flow[String].map(_.hashCode).append(PublisherDrain[Int])
      val appended: Sink[Int] = open.append(closedDrain)
      "appended.run()" shouldNot compile
      "appended.toFuture" shouldNot compile
      appended.prepend(intSeq).run
    }

    "prepend Source" in {
      val open: Flow[Int, String] = Flow[Int].map(_.toString)
      val closedFaucet: Source[Int] = Flow[String].map(_.hashCode).prepend(strSeq)
      val prepended: Source[String] = open.prepend(closedFaucet)
      "prepended.run()" shouldNot compile
      "prepended.prepend(strSeq)" shouldNot compile // TODO:ban pay attention here
      prepended.append(PublisherDrain[String]).run
    }
  }

  "Sink" should {
    val openSource: Sink[Int] =
      Flow[Int].map(_.toString).append(PublisherDrain[String])
    "accept Source" in {
      openSource.prepend(intSeq)
    }
    "not accept Sink" in {
      "openSource.toFuture" shouldNot compile // TODO:ban is this old?
    }
    "not run()" in {
      "openSource.run()" shouldNot compile
    }
  }

  "Source" should {
    val openSink: Source[String] =
      Source(Seq(1, 2, 3)).map(_.toString)
    "accept Sink" in {
      openSink.append(PublisherDrain[String])
    }
    "not accept Source" in {
      "openSink.append(intSeq)" shouldNot compile // TODO:ban pay attention here
    }
    "not run()" in {
      "openSink.run()" shouldNot compile
    }
  }

  "RunnableFlow" should {
    val closed: RunnableFlow =
      Source(Seq(1, 2, 3)).map(_.toString).append(PublisherDrain[String])
    "run" in {
      closed.run()
    }
    "not accept Source" in {
      "closed.prepend(intSeq)" shouldNot compile // TODO:ban pay attention here
    }

    "not accept Sink" in {
      "closed.toFuture" shouldNot compile // TODO:ban is this old?
    }
  }

}
