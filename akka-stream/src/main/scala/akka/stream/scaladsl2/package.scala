/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream

/**
 * TODO:ban rewrite this
 */

/**
 * Scala API: The flow DSL allows the formulation of stream transformations based on some
 * input. The starting point is called [[Faucet]] and can be a collection, an iterator,
 * a block of code which is evaluated repeatedly or a [[org.reactivestreams.Publisher]].
 * A flow with an attached `Faucet` is a [[SourcePipe]] and is constructed
 * with the `apply` methods in [[FlowFrom]].
 *
 * A flow may also be defined without an attached input `Faucet` and that is then
 * a [[Pipe]]. The `Faucet` can be attached later with [[Pipe#withFaucet]]
 * and it becomes a [[SourcePipe]].
 *
 * Transformations can appended to `SourcePipe` and `Pipe` with the operations
 * defined in [[PipeOps]]. Each DSL element produces a new flow that can be further transformed,
 * building up a description of the complete transformation pipeline.
 *
 * The output of the flow can be attached to a [[Drain]] with [[SourcePipe#withDrain]]
 * and if it also has an attached `Faucet` it becomes a [[RunnablePipe]]. In order to execute
 * this pipeline the flow must be materialized by calling [[RunnablePipe#run]] on it.
 *
 * You may also first attach the `Drain` to a `Pipe` with [[Pipe#withDrain]]
 * and then it becomes a [[SinkPipe]] and then attach the `Faucet` to make
 * it runnable.
 *
 * Flows can be wired together before they are materialized by appending or prepending them, or
 * connecting them into a [[FlowGraph]] with fan-in and fan-out elements.
 *
 * See <a href="https://github.com/reactive-streams/reactive-streams/">Reactive Streams</a> for
 * details on [[org.reactivestreams.Publisher]] and [[org.reactivestreams.Subscriber]].
 *
 * It should be noted that the streams modeled by this library are “hot”,
 * meaning that they asynchronously flow through a series of processors without
 * detailed control by the user. In particular it is not predictable how many
 * elements a given transformation step might buffer before handing elements
 * downstream, which means that transformation functions may be invoked more
 * often than for corresponding transformations on strict collections like
 * [[List]]. *An important consequence* is that elements that were produced
 * into a stream may be discarded by later processors, e.g. when using the
 * [[#take]] combinator.
 *
 * By default every operation is executed within its own [[akka.actor.Actor]]
 * to enable full pipelining of the chained set of computations. This behavior
 * is determined by the [[akka.stream.FlowMaterializer]] which is required
 * by those methods that materialize the Flow into a series of
 * [[org.reactivestreams.Processor]] instances. The returned reactive stream
 * is fully started and active.
 *
 * Use [[ImplicitFlowMaterializer]] to define an implicit [[akka.stream.FlowMaterializer]]
 * inside an [[akka.actor.Actor]].
 */
package object scaladsl2 {
}
