/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl2

import akka.stream.MaterializerSettings
import akka.stream.testkit.AkkaSpec

class FlowPrependSpec extends AkkaSpec with River {

  val settings = MaterializerSettings(system)
  implicit val materializer = FlowMaterializer(settings)

  "Flow" should {
    "prepend Flow" in riverOf[String] { subscriber ⇒
      Flow[String]
        .prepend(otherFlow)
        .prepend(IterableFaucet(elements))
        .publishTo(subscriber)
    }

    "prepend Source" in riverOf[String] { subscriber ⇒
      Flow[String]
        .prepend(otherFlow.prepend(IterableFaucet(elements)))
        .publishTo(subscriber)
    }
  }

  "Sink" should {
    "prepend Flow" in riverOf[String] { subscriber ⇒
      Flow[String]
        .append(SubscriberDrain(subscriber))
        .prepend(otherFlow)
        .prepend(Source(elements))
        .run()
    }

    "prepend Source" in riverOf[String] { subscriber ⇒
      Flow[String]
        .append(SubscriberDrain(subscriber))
        .prepend(otherFlow.prepend(IterableFaucet(elements)))
        .run()
    }
  }

}
