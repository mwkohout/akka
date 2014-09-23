/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.io2

import java.io.Closeable
import java.net.{ URLEncoder, InetSocketAddress }
import akka.actor._
import akka.io.Tcp._
import akka.io.{ IO, Tcp }
import akka.stream.impl._
import akka.stream.impl2.Ast.GenericProcessor
import akka.stream.io2.StreamTcp.IncomingTcpConnection
import akka.stream.scaladsl2.{ ProcessorFlow, PublisherSource, FlowMaterializer, FlowWithSink }
import akka.util.ByteString
import scala.util.control.NoStackTrace

/**
 * INTERNAL API
 */
private[akka] object TcpListenStreamActor {
  class TcpListenStreamException(msg: String) extends RuntimeException(msg) with NoStackTrace

  def props(bindCmd: Tcp.Bind,
            requester: ActorRef,
            connectionHandler: FlowWithSink[IncomingTcpConnection, IncomingTcpConnection],
            materializer: FlowMaterializer): Props = {
    Props(new TcpListenStreamActor(bindCmd, requester, connectionHandler)(materializer))
  }

}

/**
 * INTERNAL API
 */
private[akka] class TcpListenStreamActor(bindCmd: Tcp.Bind,
                                         requester: ActorRef,
                                         connectionHandler: FlowWithSink[IncomingTcpConnection, IncomingTcpConnection])(implicit val materializer: FlowMaterializer) extends Actor
  with Pump with Stash with Closeable {
  import akka.stream.io.TcpListenStreamActor._
  import context.system

  IO(Tcp) ! bindCmd.copy(handler = self)

  val primaryOutputs = new SimpleOutputs(self, pump = this)

  private var finished = false
  override protected def pumpFinished(): Unit = {
    if (!finished) {
      finished = true
      incomingConnections.cancel()
      primaryOutputs.complete()
      context.stop(self)
    }
  }

  override protected def pumpFailed(e: Throwable): Unit = fail(e)

  val incomingConnections: Inputs = new DefaultInputTransferStates {
    var listener: ActorRef = _
    private var closed: Boolean = false
    private var pendingConnection: (Connected, ActorRef) = null

    def waitBound: Receive = {
      case Bound(localAddress) ⇒
        listener = sender()
        nextPhase(runningPhase)
        listener ! ResumeAccepting(1)
        val publisher = ActorPublisher[IncomingTcpConnection](self)
        val mf = connectionHandler.withSource(PublisherSource(publisher)).run()
        requester ! StreamTcp.TcpServerBinding(localAddress, mf, TcpListenStreamActor.this)
        subreceive.become(running)
      case f: CommandFailed ⇒
        val ex = new TcpListenStreamException("Bind failed")
        requester ! Status.Failure(ex)
        fail(ex)
    }

    def running: Receive = {
      case c: Connected ⇒
        pendingConnection = (c, sender())
        pump()
      case f: CommandFailed ⇒
        fail(new TcpListenStreamException(s"Command [${f.cmd}] failed"))
        pump()
      case Unbind ⇒
        cancel()
        pump()
      case Unbound ⇒ // If we're unbound then just shut down
        closed = true
        pump()
    }

    override val subreceive = new SubReceive(waitBound)

    override def inputsAvailable: Boolean = pendingConnection ne null
    override def inputsDepleted: Boolean = closed && !inputsAvailable
    override def isClosed: Boolean = closed
    override def cancel(): Unit = {
      if (!closed && listener != null) listener ! Unbind
      closed = true
      pendingConnection = null
    }
    override def dequeueInputElement(): Any = {
      val elem = pendingConnection
      pendingConnection = null
      listener ! ResumeAccepting(1)
      elem
    }

  }

  final override def receive = incomingConnections.subreceive orElse primaryOutputs.subreceive

  var nameCounter: Long = 0
  def encName(localAddress: InetSocketAddress, remoteAddress: InetSocketAddress) = {
    nameCounter += 1
    s"$nameCounter-${URLEncoder.encode(localAddress.toString, "utf-8")}-${URLEncoder.encode(remoteAddress.toString, "utf-8")}"
  }

  def runningPhase = TransferPhase(primaryOutputs.NeedsDemand && incomingConnections.NeedsInput) { () ⇒
    val (connected: Connected, connection: ActorRef) = incomingConnections.dequeueInputElement()
    val tcpStreamActorCreator = (materializer: FlowMaterializer) ⇒ {
      TcpStreamActor.inboundProps(connection, materializer)
    }

    val flow = ProcessorFlow[ByteString, ByteString](List(GenericProcessor(tcpStreamActorCreator, encName(connected.localAddress, connected.remoteAddress))))
    primaryOutputs.enqueueOutputElement(StreamTcp.IncomingTcpConnection(connected.remoteAddress, flow))
  }

  def fail(e: Throwable): Unit = {
    incomingConnections.cancel()
    primaryOutputs.cancel(e)
  }

  override def close() = {
    self ! Unbind
  }
}
