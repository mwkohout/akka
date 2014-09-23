/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.io2

import akka.actor._
import akka.actor.SupervisorStrategy.Stop
import akka.io.Inet.SocketOption
import akka.io.{ Tcp, IO }
import akka.japi.Util
import akka.stream.io2.StreamTcp.IncomingTcpConnection
import akka.stream.MaterializerSettings
import akka.stream.scaladsl2._
import akka.util.ByteString
import java.io.Closeable
import java.net.InetSocketAddress
import java.net.URLEncoder
import scala.collection._
import scala.concurrent.duration.{ Duration, FiniteDuration }

object StreamTcp extends ExtensionId[StreamTcpExt] with ExtensionIdProvider {

  override def lookup = StreamTcp
  override def createExtension(system: ExtendedActorSystem): StreamTcpExt = new StreamTcpExt(system)
  override def get(system: ActorSystem): StreamTcpExt = super.get(system)

  case class OutgoingTcpConnection(remoteAddress: InetSocketAddress,
                                   localAddress: InetSocketAddress,
                                   pipeline: MaterializedFlowGraph) {
  }

  case class TcpServerBinding(localAddress: InetSocketAddress, connectionFlow: MaterializedFlow, closeable: Closeable)

  case class IncomingTcpConnection(remoteAddress: InetSocketAddress,
                                   tcpFlow: ProcessorFlow[ByteString, ByteString])

  /**
   * The Connect message is sent to the StreamTcp manager actor, which is obtained via
   * `IO(StreamTcp)`. The manager replies with a [[StreamTcp.OutgoingTcpConnection]]
   * message.
   *
   * @param pipeline the pipeline that will be connected over TCP
   * @param remoteAddress the address to connect to
   * @param localAddress optionally specifies a specific address to bind to
   * @param materializer if Some the passed in [[FlowMaterializer]] will be used during stream actor
   *                     creation, otherwise the ActorSystem's default settings will be used
   * @param options Please refer to [[akka.io.TcpSO]] for a list of all supported options.
   * @param connectTimeout the desired timeout for connection establishment, infinite means "no timeout"
   * @param idleTimeout the desired idle timeout on the connection, infinite means "no timeout"
   */
  case class Connect(pipeline: PartialFlowGraph,
                     remoteAddress: InetSocketAddress,
                     localAddress: Option[InetSocketAddress] = None,
                     materializer: Option[FlowMaterializer] = None,
                     options: immutable.Traversable[SocketOption] = Nil,
                     connectTimeout: Duration = Duration.Inf,
                     idleTimeout: Duration = Duration.Inf) {
    /**
     * Java API
     */
    def withMaterializer(materializer: FlowMaterializer): Connect =
      copy(materializer = Option(materializer))

    /**
     * Java API
     */
    def withLocalAddress(localAddress: InetSocketAddress): Connect =
      copy(localAddress = Option(localAddress))

    /**
     * Java API
     */
    def withSocketOptions(options: java.lang.Iterable[SocketOption]): Connect =
      copy(options = Util.immutableSeq(options))

    /**
     * Java API
     */
    def withConnectTimeout(connectTimeout: Duration): Connect =
      copy(connectTimeout = connectTimeout)

    /**
     * Java API
     */
    def withIdleTimeout(idleTimeout: Duration): Connect =
      copy(idleTimeout = idleTimeout)
  }

  /**
   * The Bind message is send to the StreamTcp manager actor, which is obtained via
   * `IO(StreamTcp)`, in order to bind to a listening socket. The manager
   * replies with a [[StreamTcp.TcpServerBinding]]. If the local port is set to 0 in
   * the Bind message, then the [[StreamTcp.TcpServerBinding]] message should be inspected to find
   * the actual port which was bound to.
   *
   * @param connectionHandler the flow that will handle the incoming connections
   * @param localAddress the socket address to bind to; use port zero for automatic assignment (i.e. an ephemeral port)
   * @param materializer if Some the passed in [[FlowMaterializer]] will be used during stream actor
   *                     creation, otherwise the ActorSystem's default settings will be used
   * @param backlog the number of unaccepted connections the O/S
   *                kernel will hold for this port before refusing connections.
   * @param options Please refer to [[akka.io.TcpSO]] for a list of all supported options.
   * @param idleTimeout the desired idle timeout on the accepted connections, infinite means "no timeout"
   */
  case class Bind(connectionHandler: FlowWithSink[IncomingTcpConnection, IncomingTcpConnection],
                  localAddress: InetSocketAddress,
                  materializer: Option[FlowMaterializer] = None,
                  backlog: Int = 100,
                  options: immutable.Traversable[SocketOption] = Nil,
                  idleTimeout: Duration = Duration.Inf) {

    /**
     * Java API
     */
    def withBacklog(backlog: Int): Bind = copy(backlog = backlog)

    /**
     * Java API
     */
    def withSocketOptions(options: java.lang.Iterable[SocketOption]): Bind =
      copy(options = Util.immutableSeq(options))

    /**
     * Java API
     */
    def withIdleTimeout(idleTimeout: Duration): Bind =
      copy(idleTimeout = idleTimeout)
  }

}

/**
 * Java API: Factory methods for the messages of `StreamTcp`.
 */
object StreamTcpMessage {
  /**
   * Java API: The Connect message is sent to the StreamTcp manager actor, which is obtained via
   * `StreamTcp.get(system).manager()`. The manager replies with a [[StreamTcp.OutgoingTcpConnection]]
   * message.
   *
   * @param pipeline the pipeline that will be connected over TCP
   * @param materializer the materializer that will be used during stream actor creation
   * @param remoteAddress is the address to connect to
   * @param localAddress optionally specifies a specific address to bind to
   * @param options Please refer to [[akka.io.TcpSO]] for a list of all supported options.
   * @param connectTimeout the desired timeout for connection establishment, infinite means "no timeout"
   * @param idleTimeout the desired idle timeout on the connection, infinite means "no timeout"
   */
  def connect(
    pipeline: PartialFlowGraph,
    materializer: FlowMaterializer,
    remoteAddress: InetSocketAddress,
    localAddress: InetSocketAddress,
    options: java.lang.Iterable[SocketOption],
    connectTimeout: Duration,
    idleTimeout: Duration): StreamTcp.Connect =
    StreamTcp.Connect(pipeline, remoteAddress, Option(localAddress), Option(materializer), Util.immutableSeq(options), connectTimeout, idleTimeout)

  /**
   * Java API: Message to Connect to the given `remoteAddress` without binding to a local address and without
   * specifying options.
   */
  def connect(pipeline: PartialFlowGraph, materailizer: FlowMaterializer, remoteAddress: InetSocketAddress): StreamTcp.Connect =
    StreamTcp.Connect(pipeline, remoteAddress, materializer = Option(materailizer))

  /**
   * Java API: The Bind message is send to the StreamTcp manager actor, which is obtained via
   * `StreamTcp.get(system).manager()`, in order to bind to a listening socket. The manager
   * replies with a [[StreamTcp.TcpServerBinding]]. If the local port is set to 0 in
   * the Bind message, then the [[StreamTcp.TcpServerBinding]] message should be inspected to find
   * the actual port which was bound to.
   *
   * @param localAddress the socket address to bind to; use port zero for automatic assignment (i.e. an ephemeral port)
   * @param backlog the number of unaccepted connections the O/S
   *                kernel will hold for this port before refusing connections.
   * @param options Please refer to [[akka.io.TcpSO]] for a list of all supported options.
   * @param idleTimeout the desired idle timeout on the accepted connections, infinite means "no timeout"
   */
  def bind(connectionHandler: FlowWithSink[IncomingTcpConnection, IncomingTcpConnection],
           localAddress: InetSocketAddress,
           materializer: FlowMaterializer,
           backlog: Int,
           options: java.lang.Iterable[SocketOption],
           idleTimeout: Duration): StreamTcp.Bind =
    StreamTcp.Bind(connectionHandler, localAddress, Some(materializer), backlog, Util.immutableSeq(options), idleTimeout)

  /**
   * Java API: Message to open a listening socket without specifying options.
   */
  def bind(connectionHandler: FlowWithSink[IncomingTcpConnection, IncomingTcpConnection], localAddress: InetSocketAddress): StreamTcp.Bind =
    StreamTcp.Bind(connectionHandler, localAddress)
}

/**
 * INTERNAL API
 */
private[akka] class StreamTcpExt(system: ExtendedActorSystem) extends IO.Extension {
  val manager = system.systemActorOf(Props[StreamTcpManager], name = "IO-TCP-STREAM2")
}

/**
 * INTERNAL API
 */
private[akka] class StreamTcpManager extends Actor {

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 0) {
    case _ ⇒ Stop
  }

  var nameCounter = 0
  def encName(prefix: String, address: InetSocketAddress) = {
    nameCounter += 1
    s"$prefix-$nameCounter-${URLEncoder.encode(address.toString, "utf-8")}"
  }

  def receive: Receive = {
    case c @ StreamTcp.Connect(pipeline, remoteAddress, localAddress, maybeMaterializer, options, connectTimeout, idleTimeout) ⇒
      val connTimeout = connectTimeout match {
        case x: FiniteDuration ⇒ Some(x)
        case _                 ⇒ None
      }
      val materializer = maybeMaterializer getOrElse FlowMaterializer(MaterializerSettings(context.system))

      context.actorOf(TcpStreamActor.outboundProps(
        Tcp.Connect(remoteAddress, localAddress, options, connTimeout, pullMode = true),
        requester = sender(),
        pipeline = pipeline,
        materializer = materializer), name = encName("client", remoteAddress))

    case StreamTcp.Bind(connectionHandler, localAddress, maybeMaterializer, backlog, options, idleTimeout) ⇒
      val materializer = maybeMaterializer getOrElse FlowMaterializer(MaterializerSettings(context.system))

      val publisherActor = context.actorOf(TcpListenStreamActor.props(
        Tcp.Bind(context.system.deadLetters, localAddress, backlog, options, pullMode = true),
        requester = sender(),
        connectionHandler = connectionHandler,
        materializer = materializer), name = encName("server", localAddress))
  }
}
