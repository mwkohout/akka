/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl2

import org.reactivestreams.Subscriber

import scala.language.implicitConversions
import scala.annotation.unchecked.uncheckedVariance

/**
 * TODO:ban comment
 */
trait Sink[-In] {
  /**
   * Transform this sink by prepending the given processing stages.
   */
  def prepend[T](flow: Flow[T, In]): Sink[T]

  /**
   * Connect a source to this sink, concatenating the processing steps of both.
   */
  def prepend(source: Source[In]): RunnableFlow

  def toSubscriber()(implicit materializer: FlowMaterializer): Subscriber[In @uncheckedVariance]
}
