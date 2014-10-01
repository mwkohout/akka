/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl2

import akka.actor.Props

import scala.collection.immutable
import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success, Try }
import org.reactivestreams.{ Publisher, Subscriber, Subscription }
import akka.stream.Transformer
import akka.stream.impl.{ FanoutProcessorImpl, BlackholeSubscriber }
import akka.stream.impl2.{ ActorProcessorFactory, ActorBasedFlowMaterializer }
import java.util.concurrent.atomic.AtomicReference

/**
 * This trait is a marker for a pluggable stream drain. Concrete instances should
 * implement [[DrainWithKey]] or [[SimpleDrain]], otherwise a custom [[FlowMaterializer]]
 * will have to be used to be able to attach them.
 *
 * All Drains defined in this package rely upon an [[akka.stream.impl2.ActorBasedFlowMaterializer]] being
 * made available to them in order to use the <code>attach</code> method. Other
 * FlowMaterializers can be used but must then implement the functionality of these
 * Drain nodes themselves (or construct an ActorBasedFlowMaterializer).
 */
trait Drain[-Out]

/**
 * A drain that does not need to create a user-accessible object during materialization.
 */
trait SimpleDrain[-Out] extends Drain[Out] with SinkDrain[Out] {
  /**
   * Attach this drain to the given [[org.reactivestreams.Publisher]]. Using the given
   * [[FlowMaterializer]] is completely optional, especially if this drain belongs to
   * a different Reactive Streams implementation. It is the responsibility of the
   * caller to provide a suitable FlowMaterializer that can be used for running
   * Flows if necessary.
   *
   * @param flowPublisher the Publisher to consume elements from
   * @param materializer a FlowMaterializer that may be used for creating flows
   * @param flowName the name of the current flow, which should be used in log statements or error messages
   */
  def attach(flowPublisher: Publisher[Out @uncheckedVariance], materializer: ActorBasedFlowMaterializer, flowName: String): Unit
  /**
   * This method is only used for Drains that return true from [[#isActive]], which then must
   * implement it.
   */
  def create(materializer: ActorBasedFlowMaterializer, flowName: String): Subscriber[Out] @uncheckedVariance =
    throw new UnsupportedOperationException(s"forgot to implement create() for $getClass that says isActive==true")
  /**
   * This method indicates whether this Drain can create a Subscriber instead of being
   * attached to a Publisher. This is only used if the Flow does not contain any
   * operations.
   */
  def isActive: Boolean = false

}

/**
 * A drain that will create an object during materialization that the user will need
 * to retrieve in order to access aspects of this drain (could be a completion Future
 * or a cancellation handle, etc.)
 */
trait DrainWithKey[-Out, T] extends Drain[Out] with SinkDrain[Out] {
  /**
   * Attach this drain to the given [[org.reactivestreams.Publisher]]. Using the given
   * [[FlowMaterializer]] is completely optional, especially if this drain belongs to
   * a different Reactive Streams implementation. It is the responsibility of the
   * caller to provide a suitable FlowMaterializer that can be used for running
   * Flows if necessary.
   *
   * @param flowPublisher the Publisher to consume elements from
   * @param materializer a FlowMaterializer that may be used for creating flows
   * @param flowName the name of the current flow, which should be used in log statements or error messages
   */
  def attach(flowPublisher: Publisher[Out @uncheckedVariance], materializer: ActorBasedFlowMaterializer, flowName: String): T
  /**
   * This method is only used for Drains that return true from [[#isActive]], which then must
   * implement it.
   */
  def create(materializer: ActorBasedFlowMaterializer, flowName: String): (Subscriber[Out] @uncheckedVariance, T) =
    throw new UnsupportedOperationException(s"forgot to implement create() for $getClass that says isActive==true")
  /**
   * This method indicates whether this Drain can create a Subscriber instead of being
   * attached to a Publisher. This is only used if the Flow does not contain any
   * operations.
   */
  def isActive: Boolean = false

  // these are unique keys, case class equality would break them
  final override def equals(other: Any): Boolean = super.equals(other)
  final override def hashCode: Int = super.hashCode
}

/**
 * Holds the downstream-most [[org.reactivestreams.Publisher]] interface of the materialized flow.
 * The stream will not have any subscribers attached at this point, which means that after prefetching
 * elements to fill the internal buffers it will assert back-pressure until
 * a subscriber connects and creates demand for elements to be emitted.
 */
object PublisherDrain {
  private val instance = new PublisherDrain[Nothing]
  def apply[T]: PublisherDrain[T] = instance.asInstanceOf[PublisherDrain[T]]
  def withFanout[T](initialBufferSize: Int, maximumBufferSize: Int): FanoutPublisherDrain[T] =
    new FanoutPublisherDrain[T](initialBufferSize, maximumBufferSize)
}

class PublisherDrain[Out] extends DrainWithKey[Out, Publisher[Out]] {
  def attach(flowPublisher: Publisher[Out], materializer: ActorBasedFlowMaterializer, flowName: String): Publisher[Out] = flowPublisher
  def publisher(m: MaterializedDrain): Publisher[Out] = m.getDrainFor(this)

  override def toString: String = "PublisherDrain"
}

class FanoutPublisherDrain[Out](initialBufferSize: Int, maximumBufferSize: Int) extends DrainWithKey[Out, Publisher[Out]] {
  def publisher(m: MaterializedDrain): Publisher[Out] = m.getDrainFor(this)
  override def attach(flowPublisher: Publisher[Out], materializer: ActorBasedFlowMaterializer, flowName: String): Publisher[Out] = {
    val fanoutActor = materializer.actorOf(
      Props(new FanoutProcessorImpl(materializer.settings, initialBufferSize, maximumBufferSize)), s"$flowName-fanoutPublisher")
    val fanoutProcessor = ActorProcessorFactory[Out, Out](fanoutActor)
    flowPublisher.subscribe(fanoutProcessor)
    fanoutProcessor
  }

  override def toString: String = "Fanout"
}

object FutureDrain {
  def apply[T]: FutureDrain[T] = new FutureDrain[T]
}

/**
 * Holds a [[scala.concurrent.Future]] that will be fulfilled with the first
 * thing that is signaled to this stream, which can be either an element (after
 * which the upstream subscription is canceled), an error condition (putting
 * the Future into the corresponding failed state) or the end-of-stream
 * (failing the Future with a NoSuchElementException).
 */
class FutureDrain[Out] extends DrainWithKey[Out, Future[Out]] {
  def attach(flowPublisher: Publisher[Out], materializer: ActorBasedFlowMaterializer, flowName: String): Future[Out] = {
    val (sub, f) = create(materializer, flowName)
    flowPublisher.subscribe(sub)
    f
  }
  override def isActive = true
  override def create(materializer: ActorBasedFlowMaterializer, flowName: String): (Subscriber[Out], Future[Out]) = {
    val p = Promise[Out]()
    val sub = new Subscriber[Out] { // TODO #15804 verify this using the RS TCK
      private val sub = new AtomicReference[Subscription]
      override def onSubscribe(s: Subscription): Unit =
        if (!sub.compareAndSet(null, s)) s.cancel()
        else s.request(1)
      override def onNext(t: Out): Unit = { p.trySuccess(t); sub.get.cancel() }
      override def onError(t: Throwable): Unit = p.tryFailure(t)
      override def onComplete(): Unit = p.tryFailure(new NoSuchElementException("empty stream"))
    }
    (sub, p.future)
  }

  def future(m: MaterializedDrain): Future[Out] = m.getDrainFor(this)

  override def toString: String = "FutureDrain"
}

/**
 * Attaches a subscriber to this stream which will just discard all received
 * elements.
 */
final case object BlackholeDrain extends SimpleDrain[Any] {
  override def attach(flowPublisher: Publisher[Any], materializer: ActorBasedFlowMaterializer, flowName: String): Unit =
    flowPublisher.subscribe(create(materializer, flowName))
  override def isActive: Boolean = true
  override def create(materializer: ActorBasedFlowMaterializer, flowName: String): Subscriber[Any] =
    new BlackholeSubscriber[Any](materializer.settings.maxInputBufferSize)
}

/**
 * Attaches a subscriber to this stream.
 */
final case class SubscriberDrain[Out](subscriber: Subscriber[Out]) extends SimpleDrain[Out] {
  override def attach(flowPublisher: Publisher[Out], materializer: ActorBasedFlowMaterializer, flowName: String): Unit =
    flowPublisher.subscribe(subscriber)
  override def isActive: Boolean = true
  override def create(materializer: ActorBasedFlowMaterializer, flowName: String): Subscriber[Out] = subscriber
}

object OnCompleteDrain {
  private val SuccessUnit = Success[Unit](())
}

/**
 * When the flow is completed, either through an error or normal
 * completion, apply the provided function with [[scala.util.Success]]
 * or [[scala.util.Failure]].
 */
final case class OnCompleteDrain[Out](callback: Try[Unit] ⇒ Unit) extends SimpleDrain[Out] {
  override def attach(flowPublisher: Publisher[Out], materializer: ActorBasedFlowMaterializer, flowName: String): Unit =
    Source(flowPublisher).transform("onCompleteDrain", () ⇒ new Transformer[Out, Unit] {
      override def onNext(in: Out) = Nil
      override def onError(e: Throwable) = {
        callback(Failure(e))
        throw e
      }
      override def onTermination(e: Option[Throwable]) = {
        callback(OnCompleteDrain.SuccessUnit)
        Nil
      }
    }).consume()(materializer.withNamePrefix(flowName))
}

/**
 * Invoke the given procedure for each received element. The drain holds a [[scala.concurrent.Future]]
 * that will be completed with `Success` when reaching the normal end of the stream, or completed
 * with `Failure` if there is an error is signaled in the stream.
 */
final case class ForeachDrain[Out](f: Out ⇒ Unit) extends DrainWithKey[Out, Future[Unit]] {
  override def attach(flowPublisher: Publisher[Out], materializer: ActorBasedFlowMaterializer, flowName: String): Future[Unit] = {
    val promise = Promise[Unit]()
    Source(flowPublisher).transform("foreach", () ⇒ new Transformer[Out, Unit] {
      override def onNext(in: Out) = { f(in); Nil }
      override def onError(cause: Throwable): Unit = ()
      override def onTermination(e: Option[Throwable]) = {
        e match {
          case None    ⇒ promise.success(())
          case Some(e) ⇒ promise.failure(e)
        }
        Nil
      }
    }).consume()(materializer.withNamePrefix(flowName))
    promise.future
  }
  def future(m: MaterializedDrain): Future[Unit] = m.getDrainFor(this)
}

/**
 * Invoke the given function for every received element, giving it its previous
 * output (or the given `zero` value) and the element as input. The drain holds a
 * [[scala.concurrent.Future]] that will be completed with value of the final
 * function evaluation when the input stream ends, or completed with `Failure`
 * if there is an error is signaled in the stream.
 */
final case class FoldDrain[U, Out](zero: U)(f: (U, Out) ⇒ U) extends DrainWithKey[Out, Future[U]] {
  override def attach(flowPublisher: Publisher[Out], materializer: ActorBasedFlowMaterializer, flowName: String): Future[U] = {
    val promise = Promise[U]()

    Source(flowPublisher).transform("fold", () ⇒ new Transformer[Out, U] {
      var state: U = zero
      override def onNext(in: Out): immutable.Seq[U] = { state = f(state, in); Nil }
      override def onTermination(e: Option[Throwable]) = {
        e match {
          case None    ⇒ promise.success(state)
          case Some(e) ⇒ promise.failure(e)
        }
        Nil
      }
    }).consume()(materializer.withNamePrefix(flowName))

    promise.future
  }
  def future(m: MaterializedDrain): Future[U] = m.getDrainFor(this)
}

trait MaterializedDrain {
  def getDrainFor[T](drainKey: DrainWithKey[_, T]): T
}

trait SinkDrain[-In] extends Sink[In] { self: Drain[In] ⇒
  private lazy val sinkPipe = Pipe[In].withDrain(this)

  override def prepend[T](flow: Flow[T, In]): Sink[T] = sinkPipe.prepend(flow)

  override def prepend(source: Source[In]): RunnableFlow = sinkPipe.prepend(source)

  override def toSubscriber()(implicit materializer: FlowMaterializer): Subscriber[In @uncheckedVariance] =
    sinkPipe.toSubscriber()
}