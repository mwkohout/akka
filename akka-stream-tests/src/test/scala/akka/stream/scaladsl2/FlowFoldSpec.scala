/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl2

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.stream.testkit.AkkaSpec

class FlowFoldSpec extends AkkaSpec {
  implicit val mat = FlowMaterializer()
  import system.dispatcher

  "A Fold" must {

    "fold" in {
      val input = 1 to 100
      val foldDrain = FoldDrain[Int, Int](0)(_ + _)
      val mf = Source(input).append(foldDrain).run()
      val future = foldDrain.future(mf)
      val expected = input.fold(0)(_ + _)
      Await.result(future, 5.seconds) should be(expected)
    }

  }

}