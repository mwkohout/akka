/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl2

import scala.language.existentials
import scalax.collection.edge.LkDiEdge
import scalax.collection.mutable.Graph
import scalax.collection.immutable.{ Graph ⇒ ImmutableGraph }
import org.reactivestreams.Subscriber
import akka.stream.impl.BlackholeSubscriber
import org.reactivestreams.Publisher
import akka.stream.impl2.Ast

/**
 * Fan-in and fan-out vertices in the [[FlowGraph]] implements
 * this marker interface. Edges may end at a `JunctionInPort`.
 */
sealed trait JunctionInPort[-T] {
  private[akka] def port: Int = FlowGraphInternal.UnlabeledPort
  private[akka] def vertex: FlowGraphInternal.Vertex
  type NextT
  private[akka] def next: JunctionOutPort[NextT]
}

/**
 * Fan-in and fan-out vertices in the [[FlowGraph]] implements
 * this marker interface. Edges may start at a `JunctionOutPort`.
 */
sealed trait JunctionOutPort[+T] {
  private[akka] def port: Int = FlowGraphInternal.UnlabeledPort
  private[akka] def vertex: FlowGraphInternal.Vertex
}

/**
 * INTERNAL API
 */
private[akka] object NoNext extends JunctionOutPort[Nothing] {
  override private[akka] def vertex: FlowGraphInternal.Vertex =
    throw new UnsupportedOperationException
}

/**
 * INTERNAL API
 *
 * Fan-in and fan-out vertices in the [[FlowGraph]] implements
 * this marker interface.
 */
private[akka] sealed trait Junction[T] extends JunctionInPort[T] with JunctionOutPort[T] {
  override private[akka] def port: Int = FlowGraphInternal.UnlabeledPort
  override private[akka] def vertex: FlowGraphInternal.Vertex
  override type NextT = T
  override private[akka] def next = this
}

object Merge {
  /**
   * Create a new anonymous `Merge` vertex with the specified output type.
   * Note that a `Merge` instance can only be used at one place (one vertex)
   * in the `FlowGraph`. This method creates a new instance every time it
   * is called and those instances are not `equal`.
   */
  def apply[T]: Merge[T] = new Merge[T](None)
  /**
   * Create a named `Merge` vertex with the specified output type.
   * Note that a `Merge` with a specific name can only be used at one place (one vertex)
   * in the `FlowGraph`. Calling this method several times with the same name
   * returns instances that are `equal`.
   */
  def apply[T](name: String): Merge[T] = new Merge[T](Some(name))
}
/**
 * Merge several streams, taking elements as they arrive from input streams
 * (picking randomly when several have elements ready).
 *
 * When building the [[FlowGraph]] you must connect one or more input pipes/faucets
 * and one output pipe/drain to the `Merge` vertex.
 */
final class Merge[T](override val name: Option[String]) extends FlowGraphInternal.InternalVertex with Junction[T] {
  override private[akka] val vertex = this
  override val minimumInputCount: Int = 2
  override val maximumInputCount: Int = Int.MaxValue
  override val minimumOutputCount: Int = 1
  override val maximumOutputCount: Int = 1

  override private[akka] def astNode = Ast.Merge
}

object Broadcast {
  /**
   * Create a new anonymous `Broadcast` vertex with the specified input type.
   * Note that a `Broadcast` instance can only be used at one place (one vertex)
   * in the `FlowGraph`. This method creates a new instance every time it
   * is called and those instances are not `equal`.
   */
  def apply[T]: Broadcast[T] = new Broadcast[T](None)
  /**
   * Create a named `Broadcast` vertex with the specified input type.
   * Note that a `Broadcast` with a specific name can only be used at one place (one vertex)
   * in the `FlowGraph`. Calling this method several times with the same name
   * returns instances that are `equal`.
   */
  def apply[T](name: String): Broadcast[T] = new Broadcast[T](Some(name))
}
/**
 * Fan-out the stream to several streams. Each element is produced to
 * the other streams. It will not shutdown until the subscriptions for at least
 * two downstream subscribers have been established.
 */
final class Broadcast[T](override val name: Option[String]) extends FlowGraphInternal.InternalVertex with Junction[T] {
  override private[akka] def vertex = this
  override def minimumInputCount: Int = 1
  override def maximumInputCount: Int = 1
  override def minimumOutputCount: Int = 2
  override def maximumOutputCount: Int = Int.MaxValue

  override private[akka] def astNode = Ast.Broadcast
}

object Zip {
  /**
   * Create a new anonymous `Zip` vertex with the specified input types.
   * Note that a `Zip` instance can only be used at one place (one vertex)
   * in the `FlowGraph`. This method creates a new instance every time it
   * is called and those instances are not `equal`.*
   */
  def apply[A, B]: Zip[A, B] = new Zip[A, B](None)

  /**
   * Create a named `Zip` vertex with the specified input types.
   * Note that a `Zip` instance can only be used at one place (one vertex)
   * in the `FlowGraph`. This method creates a new instance every time it
   * is called and those instances are not `equal`.*
   */
  def apply[A, B](name: String): Zip[A, B] = new Zip[A, B](Some(name))

  class Left[A, B] private[akka] (private[akka] val vertex: Zip[A, B]) extends JunctionInPort[A] {
    override private[akka] def port = 0
    type NextT = (A, B)
    override private[akka] def next = vertex.out
  }
  class Right[A, B] private[akka] (private[akka] val vertex: Zip[A, B]) extends JunctionInPort[B] {
    override private[akka] def port = 1
    type NextT = (A, B)
    override private[akka] def next = vertex.out
  }
  class Out[A, B] private[akka] (private[akka] val vertex: Zip[A, B]) extends JunctionOutPort[(A, B)]
}

/**
 * Takes two streams and outputs an output stream formed from the two input streams
 * by combining corresponding elements in pairs. If one of the two streams is
 * longer than the other, its remaining elements are ignored.
 */
final class Zip[A, B](override val name: Option[String]) extends FlowGraphInternal.InternalVertex {
  val left = new Zip.Left(this)
  val right = new Zip.Right(this)
  val out = new Zip.Out(this)

  override def minimumInputCount: Int = 2
  override def maximumInputCount: Int = 2
  override def minimumOutputCount: Int = 1
  override def maximumOutputCount: Int = 1

  override private[akka] def astNode = Ast.Zip
}

object Unzip {
  /**
   * Create a new anonymous `Unzip` vertex with the specified output types.
   * Note that a `Unzip` instance can only be used at one place (one vertex)
   * in the `FlowGraph`. This method creates a new instance every time it
   * is called and those instances are not `equal`.*
   */
  def apply[A, B]: Unzip[A, B] = new Unzip[A, B](None)

  /**
   * Create a named `Unzip` vertex with the specified output types.
   * Note that a `Unzip` instance can only be used at one place (one vertex)
   * in the `FlowGraph`. This method creates a new instance every time it
   * is called and those instances are not `equal`.*
   */
  def apply[A, B](name: String): Unzip[A, B] = new Unzip[A, B](Some(name))

  class In[A, B] private[akka] (private[akka] val vertex: Unzip[A, B]) extends JunctionInPort[(A, B)] {
    override type NextT = Nothing
    override private[akka] def next = NoNext
  }

  class Left[A, B] private[akka] (private[akka] val vertex: Unzip[A, B]) extends JunctionOutPort[A] {
    override private[akka] def port = 0
  }
  class Right[A, B] private[akka] (private[akka] val vertex: Unzip[A, B]) extends JunctionOutPort[B] {
    override private[akka] def port = 1
  }
}

/**
 * Takes a stream of pair elements and splits each pair to two output streams.
 */
final class Unzip[A, B](override val name: Option[String]) extends FlowGraphInternal.InternalVertex {
  val in = new Unzip.In(this)
  val left = new Unzip.Left(this)
  val right = new Unzip.Right(this)

  override def minimumInputCount: Int = 1
  override def maximumInputCount: Int = 1
  override def minimumOutputCount: Int = 2
  override def maximumOutputCount: Int = 2

  override private[akka] def astNode = Ast.Unzip
}

object Concat {
  /**
   * Create a new anonymous `Concat` vertex with the specified input types.
   * Note that a `Concat` instance can only be used at one place (one vertex)
   * in the `FlowGraph`. This method creates a new instance every time it
   * is called and those instances are not `equal`.*
   */
  def apply[T]: Concat[T] = new Concat[T](None)

  /**
   * Create a named `Concat` vertex with the specified input types.
   * Note that a `Concat` instance can only be used at one place (one vertex)
   * in the `FlowGraph`. This method creates a new instance every time it
   * is called and those instances are not `equal`.*
   */
  def apply[T](name: String): Concat[T] = new Concat[T](Some(name))

  class First[T] private[akka] (val vertex: Concat[T]) extends JunctionInPort[T] {
    override val port = 0
    type NextT = T
    override def next = vertex.out
  }
  class Second[T] private[akka] (val vertex: Concat[T]) extends JunctionInPort[T] {
    override val port = 1
    type NextT = T
    override def next = vertex.out
  }
  class Out[T] private[akka] (val vertex: Concat[T]) extends JunctionOutPort[T]
}

/**
 * Takes two streams and outputs an output stream formed from the two input streams
 * by consuming one stream first emitting all of its elements, then consuming the
 * second stream emitting all of its elements.
 */
final class Concat[T](override val name: Option[String]) extends FlowGraphInternal.InternalVertex {
  val first = new Concat.First(this)
  val second = new Concat.Second(this)
  val out = new Concat.Out(this)

  override def minimumInputCount: Int = 2
  override def maximumInputCount: Int = 2
  override def minimumOutputCount: Int = 1
  override def maximumOutputCount: Int = 1

  override private[akka] def astNode = Ast.Concat
}

object UndefinedDrain {
  /**
   * Create a new anonymous `UndefinedDrain` vertex with the specified input type.
   * Note that a `UndefinedDrain` instance can only be used at one place (one vertex)
   * in the `FlowGraph`. This method creates a new instance every time it
   * is called and those instances are not `equal`.
   */
  def apply[T]: UndefinedDrain[T] = new UndefinedDrain[T](None)
  /**
   * Create a named `UndefinedDrain` vertex with the specified input type.
   * Note that a `UndefinedDrain` with a specific name can only be used at one place (one vertex)
   * in the `FlowGraph`. Calling this method several times with the same name
   * returns instances that are `equal`.
   */
  def apply[T](name: String): UndefinedDrain[T] = new UndefinedDrain[T](Some(name))
}
/**
 * It is possible to define a [[PartialFlowGraph]] with output pipes that are not connected
 * yet by using this placeholder instead of the real [[Drain]]. Later the placeholder can
 * be replaced with [[FlowGraphBuilder#attachDrain]].
 */
final class UndefinedDrain[-T](override val name: Option[String]) extends FlowGraphInternal.InternalVertex {
  override def minimumInputCount: Int = 1
  override def maximumInputCount: Int = 1
  override def minimumOutputCount: Int = 0
  override def maximumOutputCount: Int = 0

  override private[akka] def astNode = throw new UnsupportedOperationException("Undefined drains cannot be materialized")
}

object UndefinedFaucet {
  /**
   * Create a new anonymous `UndefinedFaucet` vertex with the specified input type.
   * Note that a `UndefinedFaucet` instance can only be used at one place (one vertex)
   * in the `FlowGraph`. This method creates a new instance every time it
   * is called and those instances are not `equal`.
   */
  def apply[T]: UndefinedFaucet[T] = new UndefinedFaucet[T](None)
  /**
   * Create a named `UndefinedFaucet` vertex with the specified output type.
   * Note that a `UndefinedFaucet` with a specific name can only be used at one place (one vertex)
   * in the `FlowGraph`. Calling this method several times with the same name
   * returns instances that are `equal`.
   */
  def apply[T](name: String): UndefinedFaucet[T] = new UndefinedFaucet[T](Some(name))
}
/**
 * It is possible to define a [[PartialFlowGraph]] with input pipes that are not connected
 * yet by using this placeholder instead of the real [[Faucet]]. Later the placeholder can
 * be replaced with [[FlowGraphBuilder#attachFaucet]].
 */
final class UndefinedFaucet[+T](override val name: Option[String]) extends FlowGraphInternal.InternalVertex {
  override def minimumInputCount: Int = 0
  override def maximumInputCount: Int = 0
  override def minimumOutputCount: Int = 1
  override def maximumOutputCount: Int = 1

  override private[akka] def astNode = throw new UnsupportedOperationException("Undefined faucets cannot be materialized")
}

/**
 * INTERNAL API
 */
private[akka] object FlowGraphInternal {

  def UnlabeledPort = -1

  sealed trait Vertex
  case class FaucetVertex(faucet: Faucet[_]) extends Vertex {
    override def toString = faucet.toString
    // these are unique keys, case class equality would break them
    final override def equals(other: Any): Boolean = super.equals(other)
    final override def hashCode: Int = super.hashCode
  }
  case class DrainVertex(drain: Drain[_]) extends Vertex {
    override def toString = drain.toString
    // these are unique keys, case class equality would break them
    final override def equals(other: Any): Boolean = super.equals(other)
    final override def hashCode: Int = super.hashCode
  }

  sealed trait InternalVertex extends Vertex {
    def name: Option[String]

    def minimumInputCount: Int
    def maximumInputCount: Int
    def minimumOutputCount: Int
    def maximumOutputCount: Int

    private[akka] def astNode: Ast.JunctionAstNode

    final override def equals(obj: Any): Boolean =
      obj match {
        case other: InternalVertex ⇒
          if (name.isDefined) (getClass == other.getClass && name == other.name) else (this eq other)
        case _ ⇒ false
      }

    final override def hashCode: Int = name match {
      case Some(n) ⇒ n.hashCode()
      case None    ⇒ super.hashCode()
    }

    override def toString = name match {
      case Some(n) ⇒ n
      case None    ⇒ getClass.getSimpleName + "@" + Integer.toHexString(super.hashCode())
    }
  }

  // flow not part of equals/hashCode
  case class EdgeLabel(qualifier: Int)(
    val pipe: Pipe[Any, Any],
    val inputPort: Int,
    val outputPort: Int) {

    override def toString: String = pipe.toString
  }

}

/**
 * Builder of [[FlowGraph]] and [[PartialFlowGraph]].
 * Syntactic sugar is provided by [[FlowGraphImplicits]].
 */
class FlowGraphBuilder private (graph: Graph[FlowGraphInternal.Vertex, LkDiEdge]) {
  import FlowGraphInternal._

  private[akka] def this() = this(Graph.empty[FlowGraphInternal.Vertex, LkDiEdge])

  private[akka] def this(immutableGraph: ImmutableGraph[FlowGraphInternal.Vertex, LkDiEdge]) =
    this(Graph.from(edges = immutableGraph.edges.map(e ⇒ LkDiEdge(e.from.value, e.to.value)(e.label)).toIterable))

  private implicit val edgeFactory = scalax.collection.edge.LkDiEdge

  var edgeQualifier = graph.edges.size

  private var cyclesAllowed = false

  def addEdge[In, Out](faucet: Faucet[In], pipe: Pipe[In, Out], drain: JunctionInPort[Out]): this.type = {
    val faucetVertex = FaucetVertex(faucet)
    checkAddFaucetDrainPrecondition(faucetVertex)
    checkJunctionInPortPrecondition(drain)
    addGraphEdge(faucetVertex, drain.vertex, pipe, inputPort = drain.port, outputPort = UnlabeledPort)
    this
  }

  def addEdge[In, Out](faucet: UndefinedFaucet[In], pipe: Pipe[In, Out], drain: JunctionInPort[Out]): this.type = {
    checkAddFaucetDrainPrecondition(faucet)
    checkJunctionInPortPrecondition(drain)
    addGraphEdge(faucet, drain.vertex, pipe, inputPort = drain.port, outputPort = UnlabeledPort)
    this
  }

  def addEdge[In, Out](faucet: JunctionOutPort[In], pipe: Pipe[In, Out], drain: Drain[Out]): this.type = {
    val drainVertex = DrainVertex(drain)
    checkAddFaucetDrainPrecondition(drainVertex)
    checkJunctionOutPortPrecondition(faucet)
    addGraphEdge(faucet.vertex, drainVertex, pipe, inputPort = UnlabeledPort, outputPort = faucet.port)
    this
  }

  def addEdge[In, Out](faucet: JunctionOutPort[In], pipe: Pipe[In, Out], drain: UndefinedDrain[Out]): this.type = {
    checkAddFaucetDrainPrecondition(drain)
    checkJunctionOutPortPrecondition(faucet)
    addGraphEdge(faucet.vertex, drain, pipe, inputPort = UnlabeledPort, outputPort = faucet.port)
    this
  }

  def addEdge[In, Out](faucet: JunctionOutPort[In], pipe: Pipe[In, Out], drain: JunctionInPort[Out]): this.type = {
    checkJunctionOutPortPrecondition(faucet)
    checkJunctionInPortPrecondition(drain)
    addGraphEdge(faucet.vertex, drain.vertex, pipe, inputPort = drain.port, outputPort = faucet.port)
    this
  }

  def addEdge[Out](pipe: SourcePipe[Out], drain: JunctionInPort[Out]): this.type = {
    addEdge(pipe.input, Pipe(pipe.ops), drain)
    this
  }

  def addEdge[In, Out](faucet: JunctionOutPort[In], pipe: SinkPipe[In]): this.type = {
    addEdge(faucet, Pipe(pipe.ops), pipe.output)
    this
  }

  private def addGraphEdge[In, Out](from: Vertex, to: Vertex, pipe: Pipe[In, Out], inputPort: Int, outputPort: Int): Unit = {
    if (edgeQualifier == Int.MaxValue) throw new IllegalArgumentException(s"Too many edges")
    val label = EdgeLabel(edgeQualifier)(pipe.asInstanceOf[Pipe[Any, Any]], inputPort, outputPort)
    graph.addLEdge(from, to)(label)
    edgeQualifier += 1
  }

  def attachDrain[Out](token: UndefinedDrain[Out], drain: Drain[Out]): this.type = {
    graph.find(token) match {
      case Some(existing) ⇒
        require(existing.value.isInstanceOf[UndefinedDrain[_]], s"pipe already attached to a drain [${existing.value}]")
        val edge = existing.incoming.head
        graph.remove(existing)
        graph.addLEdge(edge.from.value, DrainVertex(drain))(edge.label)
      case None ⇒ throw new IllegalArgumentException(s"No matching UndefinedDrain [${token}]")
    }
    this
  }

  def attachFaucet[In](token: UndefinedFaucet[In], faucet: Faucet[In]): this.type = {
    graph.find(token) match {
      case Some(existing) ⇒
        require(existing.value.isInstanceOf[UndefinedFaucet[_]], s"pipe already attached to a faucet [${existing.value}]")
        val edge = existing.outgoing.head
        graph.remove(existing)
        graph.addLEdge(FaucetVertex(faucet), edge.to.value)(edge.label)
      case None ⇒ throw new IllegalArgumentException(s"No matching UndefinedFaucet [${token}]")
    }
    this
  }

  /**
   * Flow graphs with cycles are in general dangerous as it can result in deadlocks.
   * Therefore, cycles in the graph are by default disallowed. `IllegalArgumentException` will
   * be throw when cycles are detected. Sometimes cycles are needed and then
   * you can allow them with this method.
   */
  def allowCycles(): Unit = {
    cyclesAllowed = true
  }

  private def checkAddFaucetDrainPrecondition(node: Vertex): Unit =
    require(graph.find(node) == None, s"[$node] instance is already used in this flow graph")

  private def checkJunctionInPortPrecondition(junction: JunctionInPort[_]): Unit = {
    junction.vertex match {
      case iv: InternalVertex ⇒
        graph.find(iv) match {
          case Some(node) ⇒
            require(
              (node.inDegree + 1) <= iv.maximumInputCount,
              s"${node.value} must have at most ${iv.maximumInputCount} incoming edges")
          case _ ⇒ // ok
        }
      case _ ⇒ // ok, no checks here
    }
  }

  private def checkJunctionOutPortPrecondition(junction: JunctionOutPort[_]): Unit = {
    junction.vertex match {
      case iv: InternalVertex ⇒
        graph.find(iv) match {
          case Some(node) ⇒
            require(
              (node.outDegree + 1) <= iv.maximumOutputCount,
              s"${node.value} must have at most ${iv.maximumOutputCount} outgoing edges")
          case _ ⇒ // ok
        }
      case _ ⇒ // ok, no checks here
    }
  }

  /**
   * INTERNAL API
   */
  private[akka] def build(): FlowGraph = {
    checkPartialBuildPreconditions()
    checkBuildPreconditions()
    new FlowGraph(immutableGraph())
  }

  /**
   * INTERNAL API
   */
  private[akka] def partialBuild(): PartialFlowGraph = {
    checkPartialBuildPreconditions()
    new PartialFlowGraph(immutableGraph())
  }

  //convert it to an immutable.Graph
  private def immutableGraph(): ImmutableGraph[Vertex, LkDiEdge] =
    ImmutableGraph.from(edges = graph.edges.map(e ⇒ LkDiEdge(e.from.value, e.to.value)(e.label)).toIterable)

  private def checkPartialBuildPreconditions(): Unit = {
    if (!cyclesAllowed) graph.findCycle match {
      case None        ⇒
      case Some(cycle) ⇒ throw new IllegalArgumentException("Cycle detected, not supported yet. " + cycle)
    }
  }

  private def checkBuildPreconditions(): Unit = {
    val undefinedFaucetsDrains = graph.nodes.filter {
      _.value match {
        case _: UndefinedFaucet[_] | _: UndefinedDrain[_] ⇒ true
        case x ⇒ false
      }
    }
    if (undefinedFaucetsDrains.nonEmpty) {
      val formatted = undefinedFaucetsDrains.map(n ⇒ n.value match {
        case u: UndefinedFaucet[_] ⇒ s"$u -> ${n.outgoing.head.label} -> ${n.outgoing.head.to}"
        case u: UndefinedDrain[_]  ⇒ s"${n.incoming.head.from} -> ${n.incoming.head.label} -> $u"
      })
      throw new IllegalArgumentException("Undefined faucets or drains: " + formatted.mkString(", "))
    }

    graph.nodes.foreach { node ⇒
      node.value match {
        case v: InternalVertex ⇒
          require(
            node.inDegree >= v.minimumInputCount,
            s"$v must have at least ${v.minimumInputCount} incoming edges")
          require(
            node.inDegree <= v.maximumInputCount,
            s"$v must have at most ${v.maximumInputCount} incoming edges")
          require(
            node.outDegree >= v.minimumOutputCount,
            s"$v must have at least ${v.minimumOutputCount} outgoing edges")
          require(
            node.outDegree <= v.maximumOutputCount,
            s"$v must have at most ${v.maximumOutputCount} outgoing edges")
        case _ ⇒ // no check for other node types
      }
    }

    require(graph.nonEmpty, "Graph must not be empty")
    require(graph.exists(graph having ((node = { n ⇒ n.isLeaf && n.diSuccessors.isEmpty }))),
      "Graph must have at least one drain")
    require(graph.exists(graph having ((node = { n ⇒ n.isLeaf && n.diPredecessors.isEmpty }))),
      "Graph must have at least one faucet")

    require(graph.isConnected, "Graph must be connected")
  }

}

/**
 * Build a [[FlowGraph]] by starting with one of the `apply` methods.
 * Syntactic sugar is provided by [[FlowGraphImplicits]].
 *
 * `IllegalArgumentException` is throw if the built graph is invalid.
 */
object FlowGraph {
  /**
   * Build a [[FlowGraph]] from scratch.
   */
  def apply(block: FlowGraphBuilder ⇒ Unit): FlowGraph =
    apply(ImmutableGraph.empty[FlowGraphInternal.Vertex, LkDiEdge])(block)

  /**
   * Continue building a [[FlowGraph]] from an existing `PartialFlowGraph`.
   * For example you can attach undefined faucets and drains with
   * [[FlowGraphBuilder#attachFaucet]] and [[FlowGraphBuilder#attachDrain]]
   */
  def apply(partialFlowGraph: PartialFlowGraph)(block: FlowGraphBuilder ⇒ Unit): FlowGraph =
    apply(partialFlowGraph.graph)(block)

  /**
   * Continue building a [[FlowGraph]] from an existing `FlowGraph`.
   * For example you can connect more output flows to a [[Broadcast]] vertex.
   */
  def apply(flowGraph: FlowGraph)(block: FlowGraphBuilder ⇒ Unit): FlowGraph =
    apply(flowGraph.graph)(block)

  private def apply(graph: ImmutableGraph[FlowGraphInternal.Vertex, LkDiEdge])(block: FlowGraphBuilder ⇒ Unit): FlowGraph = {
    val builder = new FlowGraphBuilder(graph)
    block(builder)
    builder.build()
  }
}

/**
 * Concrete flow graph that can be materialized with [[#run]].
 *
 * Build a `FlowGraph` by starting with one of the `apply` methods in
 * in [[FlowGraph$ companion object]]. Syntactic sugar is provided by [[FlowGraphImplicits]].
 */
class FlowGraph private[akka] (private[akka] val graph: ImmutableGraph[FlowGraphInternal.Vertex, LkDiEdge]) {
  import FlowGraphInternal._

  /**
   * Materialize the `FlowGraph` and attach all drains and faucets.
   */
  def run()(implicit materializer: FlowMaterializer): MaterializedPipeGraph = {
    import scalax.collection.GraphTraversal._

    // start with drains
    val startingNodes = graph.nodes.filter(n ⇒ n.isLeaf && n.diSuccessors.isEmpty)

    case class Memo(visited: Set[graph.EdgeT] = Set.empty,
                    downstreamSubscriber: Map[graph.EdgeT, Subscriber[Any]] = Map.empty,
                    upstreamPublishers: Map[graph.EdgeT, Publisher[Any]] = Map.empty,
                    faucets: Map[FaucetVertex, SinkPipe[Any]] = Map.empty,
                    materializedDrains: Map[DrainWithKey[_, _], Any] = Map.empty)

    val result = startingNodes.foldLeft(Memo()) {
      case (memo, start) ⇒

        val traverser = graph.innerEdgeTraverser(start, parameters = Parameters(direction = Predecessors, kind = BreadthFirst),
          ordering = graph.defaultEdgeOrdering)
        traverser.foldLeft(memo) {
          case (memo, edge) ⇒
            if (memo.visited(edge)) {
              memo
            } else {
              val pipe = edge.label.asInstanceOf[EdgeLabel].pipe

              // returns the materialized drain, if any
              def connectToDownstream(publisher: Publisher[Any]): Option[(DrainWithKey[_, _], Any)] = {
                val f = pipe.withFaucet(PublisherFaucet(publisher))
                edge.to.value match {
                  case DrainVertex(drain: DrainWithKey[_, _]) ⇒
                    val mf = f.withDrain(drain.asInstanceOf[Drain[Any]]).run()
                    Some(drain -> mf.getDrainFor(drain))
                  case DrainVertex(drain) ⇒
                    f.withDrain(drain.asInstanceOf[Drain[Any]]).run()
                    None
                  case _ ⇒
                    f.withDrain(SubscriberDrain(memo.downstreamSubscriber(edge))).run()
                    None
                }
              }

              edge.from.value match {
                case src: FaucetVertex ⇒
                  val f = pipe.withDrain(SubscriberDrain(memo.downstreamSubscriber(edge)))
                  // connect the faucet with the pipe later
                  memo.copy(visited = memo.visited + edge,
                    faucets = memo.faucets.updated(src, f))

                case v: InternalVertex ⇒
                  if (memo.upstreamPublishers.contains(edge)) {
                    // vertex already materialized
                    val materializedDrain = connectToDownstream(memo.upstreamPublishers(edge))
                    memo.copy(
                      visited = memo.visited + edge,
                      materializedDrains = memo.materializedDrains ++ materializedDrain)
                  } else {

                    val op = v.astNode
                    val (subscribers, publishers) =
                      materializer.materializeJunction[Any, Any](op, edge.from.inDegree, edge.from.outDegree)
                    // TODO: Check for gaps in port numbers
                    val edgeSubscribers =
                      edge.from.incoming.toSeq.sortBy(_.label.asInstanceOf[EdgeLabel].inputPort).zip(subscribers)
                    val edgePublishers =
                      edge.from.outgoing.toSeq.sortBy(_.label.asInstanceOf[EdgeLabel].outputPort).zip(publishers).toMap
                    val publisher = edgePublishers(edge)
                    val materializedDrain = connectToDownstream(publisher)
                    memo.copy(
                      visited = memo.visited + edge,
                      downstreamSubscriber = memo.downstreamSubscriber ++ edgeSubscribers,
                      upstreamPublishers = memo.upstreamPublishers ++ edgePublishers,
                      materializedDrains = memo.materializedDrains ++ materializedDrain)
                  }

              }
            }

        }

    }

    // connect all input faucets as the last thing
    val materializedFaucets = result.faucets.foldLeft(Map.empty[FaucetWithKey[_, _], Any]) {
      case (acc, (FaucetVertex(src), pipe)) ⇒
        val mf = pipe.withFaucet(src).run()
        src match {
          case srcKey: FaucetWithKey[_, _] ⇒ acc.updated(srcKey, mf.getFaucetFor(srcKey))
          case _                           ⇒ acc
        }
    }

    new MaterializedPipeGraph(materializedFaucets, result.materializedDrains)
  }

}

/**
 * Build a [[PartialFlowGraph]] by starting with one of the `apply` methods.
 * Syntactic sugar is provided by [[FlowGraphImplicits]].
 *
 * `IllegalArgumentException` is throw if the built graph is invalid.
 */
object PartialFlowGraph {
  /**
   * Build a [[PartialFlowGraph]] from scratch.
   */
  def apply(block: FlowGraphBuilder ⇒ Unit): PartialFlowGraph =
    apply(ImmutableGraph.empty[FlowGraphInternal.Vertex, LkDiEdge])(block)

  /**
   * Continue building a [[PartialFlowGraph]] from an existing `PartialFlowGraph`.
   */
  def apply(partialFlowGraph: PartialFlowGraph)(block: FlowGraphBuilder ⇒ Unit): PartialFlowGraph =
    apply(partialFlowGraph.graph)(block)

  /**
   * Continue building a [[PartialFlowGraph]] from an existing `PartialFlowGraph`.
   */
  def apply(flowGraph: FlowGraph)(block: FlowGraphBuilder ⇒ Unit): PartialFlowGraph =
    apply(flowGraph.graph)(block)

  private def apply(graph: ImmutableGraph[FlowGraphInternal.Vertex, LkDiEdge])(block: FlowGraphBuilder ⇒ Unit): PartialFlowGraph = {
    val builder = new FlowGraphBuilder(graph)
    block(builder)
    builder.partialBuild()
  }

}

/**
 * `PartialFlowGraph` may have faucets and drains that are not attached, and it can therefore not
 * be `run` until those are attached.
 *
 * Build a `PartialFlowGraph` by starting with one of the `apply` methods in
 * in [[FlowGraph$ companion object]]. Syntactic sugar is provided by [[FlowGraphImplicits]].
 */
class PartialFlowGraph private[akka] (private[akka] val graph: ImmutableGraph[FlowGraphInternal.Vertex, LkDiEdge]) {
  import FlowGraphInternal._

  def undefinedFaucets: Set[UndefinedFaucet[_]] =
    graph.nodes.collect {
      case n if n.value.isInstanceOf[UndefinedFaucet[_]] ⇒ n.value.asInstanceOf[UndefinedFaucet[_]]
    }(collection.breakOut)

  def undefinedDrains: Set[UndefinedDrain[_]] =
    graph.nodes.collect {
      case n if n.value.isInstanceOf[UndefinedDrain[_]] ⇒ n.value.asInstanceOf[UndefinedDrain[_]]
    }(collection.breakOut)

}

/**
 * Returned by [[FlowGraph#run]] and can be used as parameter to the
 * accessor method to retrieve the materialized `Faucet` or `Drain`, e.g.
 * [[SubscriberFaucet#subscriber]] or [[PublisherDrain#publisher]].
 */
class MaterializedPipeGraph(materializedFaucets: Map[FaucetWithKey[_, _], Any], materializedDrains: Map[DrainWithKey[_, _], Any])
  extends MaterializedFaucet with MaterializedDrain {

  /**
   * Do not call directly. Use accessor method in the concrete `Faucet`, e.g. [[SubscriberFaucet#subscriber]].
   */
  override def getFaucetFor[T](key: FaucetWithKey[_, T]): T =
    materializedFaucets.get(key) match {
      case Some(matFaucet) ⇒ matFaucet.asInstanceOf[T]
      case None ⇒
        throw new IllegalArgumentException(s"Faucet key [$key] doesn't exist in this flow graph")
    }

  /**
   * Do not call directly. Use accessor method in the concrete `Drain`, e.g. [[PublisherDrain#publisher]].
   */
  def getDrainFor[T](key: DrainWithKey[_, T]): T =
    materializedDrains.get(key) match {
      case Some(matDrain) ⇒ matDrain.asInstanceOf[T]
      case None ⇒
        throw new IllegalArgumentException(s"Drain key [$key] doesn't exist in this flow graph")
    }
}

/**
 * Implicit conversions that provides syntactic sugar for building flow graphs.
 */
object FlowGraphImplicits {
  implicit class FaucetOps[In](val faucet: Faucet[In]) extends AnyVal {
    def ~>[Out](pipe: Pipe[In, Out])(implicit builder: FlowGraphBuilder): FaucetNextStep[In, Out] = {
      new FaucetNextStep(faucet, pipe, builder)
    }

    def ~>(drain: JunctionInPort[In])(implicit builder: FlowGraphBuilder): JunctionOutPort[drain.NextT] = {
      builder.addEdge(faucet, Pipe.empty[In], drain)
      drain.next
    }
  }

  class FaucetNextStep[In, Out](faucet: Faucet[In], pipe: Pipe[In, Out], builder: FlowGraphBuilder) {
    def ~>(drain: JunctionInPort[Out]): JunctionOutPort[drain.NextT] = {
      builder.addEdge(faucet, pipe, drain)
      drain.next
    }
  }

  implicit class JunctionOps[In](val junction: JunctionOutPort[In]) extends AnyVal {
    def ~>[Out](pipe: Pipe[In, Out])(implicit builder: FlowGraphBuilder): JunctionNextStep[In, Out] = {
      new JunctionNextStep(junction, pipe, builder)
    }

    def ~>(drain: Drain[In])(implicit builder: FlowGraphBuilder): Unit =
      builder.addEdge(junction, Pipe.empty[In], drain)

    def ~>(drain: UndefinedDrain[In])(implicit builder: FlowGraphBuilder): Unit =
      builder.addEdge(junction, Pipe.empty[In], drain)

    def ~>(drain: JunctionInPort[In])(implicit builder: FlowGraphBuilder): JunctionOutPort[drain.NextT] = {
      builder.addEdge(junction, Pipe.empty[In], drain)
      drain.next
    }

    def ~>(pipe: SinkPipe[In])(implicit builder: FlowGraphBuilder): Unit =
      builder.addEdge(junction, pipe)
  }

  class JunctionNextStep[In, Out](junction: JunctionOutPort[In], pipe: Pipe[In, Out], builder: FlowGraphBuilder) {
    def ~>(drain: JunctionInPort[Out]): JunctionOutPort[drain.NextT] = {
      builder.addEdge(junction, pipe, drain)
      drain.next
    }

    def ~>(drain: Drain[Out]): Unit = {
      builder.addEdge(junction, pipe, drain)
    }

    def ~>(drain: UndefinedDrain[Out]): Unit = {
      builder.addEdge(junction, pipe, drain)
    }
  }

  implicit class SourcePipeOps[Out](val pipe: SourcePipe[Out]) extends AnyVal {
    def ~>(drain: JunctionInPort[Out])(implicit builder: FlowGraphBuilder): JunctionOutPort[drain.NextT] = {
      builder.addEdge(pipe, drain)
      drain.next
    }
  }

  implicit class UndefinedFaucetOps[In](val faucet: UndefinedFaucet[In]) extends AnyVal {
    def ~>[Out](pipe: Pipe[In, Out])(implicit builder: FlowGraphBuilder): UndefinedFaucetNextStep[In, Out] = {
      new UndefinedFaucetNextStep(faucet, pipe, builder)
    }

    def ~>(drain: JunctionInPort[In])(implicit builder: FlowGraphBuilder): JunctionOutPort[drain.NextT] = {
      builder.addEdge(faucet, Pipe.empty[In], drain)
      drain.next
    }

  }

  class UndefinedFaucetNextStep[In, Out](faucet: UndefinedFaucet[In], pipe: Pipe[In, Out], builder: FlowGraphBuilder) {
    def ~>(drain: JunctionInPort[Out]): JunctionOutPort[drain.NextT] = {
      builder.addEdge(faucet, pipe, drain)
      drain.next
    }
  }

}
