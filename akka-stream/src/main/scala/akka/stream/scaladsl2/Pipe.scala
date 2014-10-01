/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl2

import scala.collection.immutable
import scala.collection.immutable
import akka.stream.impl2.Ast._
import org.reactivestreams._
import scala.concurrent.Future
import akka.stream.Transformer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.Duration
import akka.util.Collections.EmptyImmutableSeq
import akka.stream.TimerTransformer
import akka.stream.OverflowStrategy

import scala.annotation.unchecked.uncheckedVariance
import scala.language.higherKinds
import scala.language.existentials

object PipeOps {
  private case object TakeWithinTimerKey
  private case object DropWithinTimerKey
  private case object GroupedWithinTimerKey

  private val takeCompletedTransformer: Transformer[Any, Any] = new Transformer[Any, Any] {
    override def onNext(elem: Any) = Nil
    override def isComplete = true
  }

  private val identityTransformer: Transformer[Any, Any] = new Transformer[Any, Any] {
    override def onNext(elem: Any) = List(elem)
  }
}

/**
 * Scala API: Operations offered by flows with a free output side: the DSL flows left-to-right only.
 */
trait PipeOps[+Out] extends FlowOps[Out] {
  import PipeOps._
  type Repr[+O]

  // Storing ops in reverse order
  protected def andThen[U](op: AstNode): Repr[U]

  override def map[T](f: Out ⇒ T): Repr[T] =
    transform("map", () ⇒ new Transformer[Out, T] {
      override def onNext(in: Out) = List(f(in))
    })

  override def mapConcat[T](f: Out ⇒ immutable.Seq[T]): Repr[T] =
    transform("mapConcat", () ⇒ new Transformer[Out, T] {
      override def onNext(in: Out) = f(in)
    })

  override def mapAsync[T](f: Out ⇒ Future[T]): Repr[T] =
    andThen(MapAsync(f.asInstanceOf[Any ⇒ Future[Any]]))

  override def mapAsyncUnordered[T](f: Out ⇒ Future[T]): Repr[T] =
    andThen(MapAsyncUnordered(f.asInstanceOf[Any ⇒ Future[Any]]))

  override def filter(p: Out ⇒ Boolean): Repr[Out] =
    transform("filter", () ⇒ new Transformer[Out, Out] {
      override def onNext(in: Out) = if (p(in)) List(in) else Nil
    })

  override def collect[T](pf: PartialFunction[Out, T]): Repr[T] =
    transform("collect", () ⇒ new Transformer[Out, T] {
      override def onNext(in: Out) = if (pf.isDefinedAt(in)) List(pf(in)) else Nil
    })

  override def grouped(n: Int): Repr[immutable.Seq[Out]] = {
    require(n > 0, "n must be greater than 0")
    transform("grouped", () ⇒ new Transformer[Out, immutable.Seq[Out]] {
      var buf: Vector[Out] = Vector.empty
      override def onNext(in: Out) = {
        buf :+= in
        if (buf.size == n) {
          val group = buf
          buf = Vector.empty
          List(group)
        } else
          Nil
      }
      override def onTermination(e: Option[Throwable]) = if (buf.isEmpty) Nil else List(buf)
    })
  }

  override def groupedWithin(n: Int, d: FiniteDuration): Repr[immutable.Seq[Out]] = {
    require(n > 0, "n must be greater than 0")
    require(d > Duration.Zero)
    timerTransform("groupedWithin", () ⇒ new TimerTransformer[Out, immutable.Seq[Out]] {
      schedulePeriodically(GroupedWithinTimerKey, d)
      var buf: Vector[Out] = Vector.empty

      override def onNext(in: Out) = {
        buf :+= in
        if (buf.size == n) {
          // start new time window
          schedulePeriodically(GroupedWithinTimerKey, d)
          emitGroup()
        } else Nil
      }
      override def onTermination(e: Option[Throwable]) = if (buf.isEmpty) Nil else List(buf)
      override def onTimer(timerKey: Any) = emitGroup()
      private def emitGroup(): immutable.Seq[immutable.Seq[Out]] =
        if (buf.isEmpty) EmptyImmutableSeq
        else {
          val group = buf
          buf = Vector.empty
          List(group)
        }
    })
  }

  override def drop(n: Int): Repr[Out] =
    transform("drop", () ⇒ new Transformer[Out, Out] {
      var delegate: Transformer[Out, Out] =
        if (n <= 0) identityTransformer.asInstanceOf[Transformer[Out, Out]]
        else new Transformer[Out, Out] {
          var c = n
          override def onNext(in: Out) = {
            c -= 1
            if (c == 0)
              delegate = identityTransformer.asInstanceOf[Transformer[Out, Out]]
            Nil
          }
        }

      override def onNext(in: Out) = delegate.onNext(in)
    })

  override def dropWithin(d: FiniteDuration): Repr[Out] =
    timerTransform("dropWithin", () ⇒ new TimerTransformer[Out, Out] {
      scheduleOnce(DropWithinTimerKey, d)

      var delegate: Transformer[Out, Out] =
        new Transformer[Out, Out] {
          override def onNext(in: Out) = Nil
        }

      override def onNext(in: Out) = delegate.onNext(in)
      override def onTimer(timerKey: Any) = {
        delegate = identityTransformer.asInstanceOf[Transformer[Out, Out]]
        Nil
      }
    })

  override def take(n: Int): Repr[Out] =
    transform("take", () ⇒ new Transformer[Out, Out] {
      var delegate: Transformer[Out, Out] =
        if (n <= 0) takeCompletedTransformer.asInstanceOf[Transformer[Out, Out]]
        else new Transformer[Out, Out] {
          var c = n
          override def onNext(in: Out) = {
            c -= 1
            if (c == 0)
              delegate = takeCompletedTransformer.asInstanceOf[Transformer[Out, Out]]
            List(in)
          }
        }

      override def onNext(in: Out) = delegate.onNext(in)
      override def isComplete = delegate.isComplete
    })

  override def takeWithin(d: FiniteDuration): Repr[Out] =
    timerTransform("takeWithin", () ⇒ new TimerTransformer[Out, Out] {
      scheduleOnce(TakeWithinTimerKey, d)

      var delegate: Transformer[Out, Out] = identityTransformer.asInstanceOf[Transformer[Out, Out]]

      override def onNext(in: Out) = delegate.onNext(in)
      override def isComplete = delegate.isComplete
      override def onTimer(timerKey: Any) = {
        delegate = takeCompletedTransformer.asInstanceOf[Transformer[Out, Out]]
        Nil
      }
    })

  override def conflate[S](seed: Out ⇒ S, aggregate: (S, Out) ⇒ S): Repr[S] =
    andThen(Conflate(seed.asInstanceOf[Any ⇒ Any], aggregate.asInstanceOf[(Any, Any) ⇒ Any]))

  override def expand[S, U](seed: Out ⇒ S, extrapolate: S ⇒ (U, S)): Repr[U] =
    andThen(Expand(seed.asInstanceOf[Any ⇒ Any], extrapolate.asInstanceOf[Any ⇒ (Any, Any)]))

  override def buffer(size: Int, overflowStrategy: OverflowStrategy): Repr[Out] = {
    require(size > 0, s"Buffer size must be larger than zero but was [$size]")
    andThen(Buffer(size, overflowStrategy))
  }

  override def transform[T](name: String, mkTransformer: () ⇒ Transformer[Out, T]): Repr[T] = {
    andThen(Transform(name, mkTransformer.asInstanceOf[() ⇒ Transformer[Any, Any]]))
  }

  override def prefixAndTail[U >: Out](n: Int): Repr[(immutable.Seq[Out], SourcePipe[U])] =
    andThen(PrefixAndTail(n))

  override def groupBy[K, U >: Out](f: Out ⇒ K): Repr[(K, SourcePipe[U])] =
    andThen(GroupBy(f.asInstanceOf[Any ⇒ Any]))

  override def splitWhen[U >: Out](p: Out ⇒ Boolean): Repr[SourcePipe[U]] =
    andThen(SplitWhen(p.asInstanceOf[Any ⇒ Boolean]))

  override def flatten[U](strategy: FlattenStrategy[Out, U]): Repr[U] = strategy match {
    case _: FlattenStrategy.Concat[Out] ⇒ andThen(ConcatAll)
    case _                              ⇒ throw new IllegalArgumentException(s"Unsupported flattening strategy [${strategy.getClass.getSimpleName}]")
  }

  override def timerTransform[U](name: String, mkTransformer: () ⇒ TimerTransformer[Out, U]): Repr[U] =
    andThen(TimerTransform(name, mkTransformer.asInstanceOf[() ⇒ TimerTransformer[Any, Any]]))
}

object Pipe {
  /**
   * Helper to create `Pipe` without [[Faucet]].
   * Example usage: `Pipe[Int]`
   */
  def apply[T]: Pipe[T, T] = Pipe.empty[T]

  private val emptyInstance = Pipe[Any, Any](ops = Nil)
  def empty[T]: Pipe[T, T] = emptyInstance.asInstanceOf[Pipe[T, T]]

  /**
   * FIXME Remove this when the FlowGraph DSL supports Sink/Source/Flow as well as Pipes
   */
  def apply[T](publisher: Publisher[T]): SourcePipe[T] = Pipe[T].withFaucet(PublisherFaucet(publisher))
  def apply[T](iterator: Iterator[T]): SourcePipe[T] = Pipe[T].withFaucet(IteratorFaucet(iterator))
  def apply[T](iterable: immutable.Iterable[T]): SourcePipe[T] = Pipe[T].withFaucet(IterableFaucet(iterable))
  def apply[T](f: () ⇒ Option[T]): SourcePipe[T] = Pipe[T].withFaucet(ThunkFaucet(f))
  def apply[T](future: Future[T]): SourcePipe[T] = Pipe[T].withFaucet(FutureFaucet(future))
  def apply[T](initialDelay: FiniteDuration, interval: FiniteDuration, tick: () ⇒ T): SourcePipe[T] =
    Pipe[T].withFaucet(TickFaucet(initialDelay, interval, tick))
}

/**
 * Flow without attached input and without attached output, can be used as a `Processor`.
 */
final case class Pipe[-In, +Out] private[scaladsl2] (ops: List[AstNode]) extends Flow[In, Out] with PipeOps[Out] {
  override type Repr[+O] = Pipe[In @uncheckedVariance, O]

  override protected def andThen[U](op: AstNode): Repr[U] = this.copy(ops = op :: ops)

  def withDrain(out: Drain[Out]): SinkPipe[In] = SinkPipe(out, ops)
  def withFaucet(in: Faucet[In]): SourcePipe[Out] = SourcePipe(in, ops)

  override def prepend[T](flow: Flow[T, In]): Flow[T, Out] = flow match {
    case p: Pipe[T, In] ⇒ Pipe(ops ::: p.ops)
    case _              ⇒ flow.append(this)
  }

  override def prepend(source: Source[In]): Source[Out] = source.append(this)

  override def append[T](flow: Flow[Out, T]): Flow[In, T] = flow match {
    case p: Pipe[T, In] ⇒ Pipe(p.ops ++: ops)
    case _              ⇒ flow.prepend(this)
  }

  override def append(sink: Sink[Out]): Sink[In] = sink.prepend(this)
}

/**
 *  Flow with attached output, can be used as a `Subscriber`.
 */
final case class SinkPipe[-In] private[scaladsl2] (output: Drain[_], ops: List[AstNode]) extends Sink[In] {

  def withFaucet(in: Faucet[In]): RunnablePipe = RunnablePipe(in, output, ops)

  override def prepend[T](flow: Flow[T, In]): Sink[T] = flow match {
    case p: Pipe[T, In] ⇒ SinkPipe(output, ops ::: p.ops)
    case _              ⇒ flow.append(this)
  }

  override def prepend(source: Source[In]): RunnableFlow = source match {
    case sp: SourcePipe[In] ⇒ RunnablePipe(sp.input, output, ops ::: sp.ops)
    case _                  ⇒ source.append(this)
  }

  override def toSubscriber()(implicit materializer: FlowMaterializer): Subscriber[In @uncheckedVariance] = {
    val subIn = SubscriberFaucet[In]()
    val mf = withFaucet(subIn).run()
    subIn.subscriber(mf)
  }
}

/**
 * Pipe with attached input, can be used as a `Publisher`.
 */
final case class SourcePipe[+Out] private[scaladsl2] (input: Faucet[_], ops: List[AstNode]) extends Source[Out] with PipeOps[Out] {
  override type Repr[+O] = SourcePipe[O]

  override protected def andThen[U](op: AstNode): Repr[U] = SourcePipe(input, op :: ops)

  def withDrain(out: Drain[Out]): RunnablePipe = RunnablePipe(input, out, ops)

  override def append[T](flow: Flow[Out, T]): Source[T] = flow match {
    case p: Pipe[Out, T] ⇒ SourcePipe(input, p.ops ++: ops)
    case _               ⇒ flow.prepend(this)
  }

  override def append(sink: Sink[Out]): RunnableFlow = sink match {
    case sp: SinkPipe[Out] ⇒ RunnablePipe(input, sp.output, sp.ops ++: ops)
    case _                 ⇒ sink.prepend(this)
  }

  override def toPublisher()(implicit materializer: FlowMaterializer): Publisher[Out @uncheckedVariance] = {
    val pubOut = PublisherDrain[Out]
    val mf = withDrain(pubOut).run()
    pubOut.publisher(mf)
  }

  override def toFanoutPublisher(initialBufferSize: Int, maximumBufferSize: Int)(implicit materializer: FlowMaterializer): Publisher[Out @uncheckedVariance] = {
    val pubOut = PublisherDrain.withFanout[Out](initialBufferSize, maximumBufferSize)
    val mf = withDrain(pubOut).run()
    pubOut.publisher(mf)
  }

  override def publishTo(subscriber: Subscriber[Out @uncheckedVariance])(implicit materializer: FlowMaterializer): Unit =
    toPublisher().subscribe(subscriber)

  override def consume()(implicit materializer: FlowMaterializer): Unit =
    withDrain(BlackholeDrain).run()
}

/**
 * Pipe with attached input and output, can be executed.
 */
final case class RunnablePipe private[scaladsl2] (input: Faucet[_], output: Drain[_], ops: List[AstNode]) extends RunnableFlow {
  def run()(implicit materializer: FlowMaterializer): MaterializedPipe =
    materializer.materialize(input, output, ops)
}

/**
 * Returned by [[RunnablePipe#run]] and can be used as parameter to the
 * accessor method to retrieve the materialized `Faucet` or `Drain`, e.g.
 * [[SubscriberFaucet#subscriber]] or [[PublisherDrain#publisher]].
 */
class MaterializedPipe(faucetKey: AnyRef, matFaucet: Any, drainKey: AnyRef, matDrain: Any) extends MaterializedFlow {
  /**
   * Do not call directly. Use accessor method in the concrete `Faucet`, e.g. [[SubscriberFaucet#subscriber]].
   */
  override def getFaucetFor[T](key: FaucetWithKey[_, T]): T =
    if (key == faucetKey) matFaucet.asInstanceOf[T]
    else throw new IllegalArgumentException(s"Faucet key [$key] doesn't match the faucet [$faucetKey] of this flow")

  /**
   * Do not call directly. Use accessor method in the concrete `Drain`, e.g. [[PublisherDrain#publisher]].
   */
  def getDrainFor[T](key: DrainWithKey[_, T]): T =
    if (key == drainKey) matDrain.asInstanceOf[T]
    else throw new IllegalArgumentException(s"Drain key [$key] doesn't match the drain [$drainKey] of this flow")
}
