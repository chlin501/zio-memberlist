package zio.memberlist

import zio.clock.Clock
import zio.logging.Logger
import zio.memberlist.encoding.MsgPackCodec
import zio.memberlist.protocols.messages.Compound
import zio.memberlist.state.Nodes
import zio.memberlist.transport.MemberlistTransport
import zio.{Chunk, Fiber, IO, UIO, ZIO, ZLayer}

final class MessageSink(
  broadcast: Broadcast,
  transport: MemberlistTransport,
  nodes: Nodes,
  clock: Clock.Service,
  logger: Logger[String]
) {

  /**
   * Sends message to target.
   */
  def send(msg: Message[Chunk[Byte]]): IO[Error, Unit] =
    msg match {
      case Message.NoResponse                                => ZIO.unit
      case Message.BestEffortByName(nodeName, message)       =>
        for {
          broadcast    <- broadcast.broadcast(message.size)
          withPiggyback = Compound(Chunk.fromIterable(message :: broadcast))
          chunk        <- MsgPackCodec[Compound].encode(withPiggyback)
          nodeAddress  <- nodes.nodeAddress(nodeName).commit
          _            <- transport.sendBestEffort(nodeAddress, chunk)
        } yield ()
      case Message.BestEffortByAddress(nodeAddress, message) =>
        for {
          chunk <- MsgPackCodec[Compound].encode(Compound(Chunk.single(message)))
          _     <- transport.sendBestEffort(nodeAddress, chunk)
        } yield ()
      case Message.ReliableByConnection(id, chunk)           =>
        transport.sendReliably(id, chunk)
      case Message.ReliableByAddress(nodeAddress, chunk)     =>
        transport.sendReliably(nodeAddress, chunk)
      case Message.ReliableByName(nodeName, chunk)           =>
        nodes.nodeAddress(nodeName).commit.flatMap(transport.sendReliably(_, chunk))
      case msg: Message.Batch[Chunk[Byte] @unchecked]        =>
        val (broadcast, rest) =
          (msg.first :: msg.second :: msg.rest.toList).partition(_.isInstanceOf[Message.Broadcast[_]])
        ZIO.foreach_(broadcast)(send) *>
          ZIO.foreach_(rest)(send)
      case msg @ Message.Broadcast(_)                        =>
        broadcast.add(msg.asInstanceOf[Message.Broadcast[Chunk[Byte]]])
      case Message.WithTimeout(message, action, timeout)     =>
        send(message) *> clock.sleep(timeout) *> action.flatMap(send).unit
    }

  def process(
    protocol: Protocol[Chunk[Byte]]
  ): UIO[Fiber.Runtime[Nothing, Unit]] =
    transport.receiveBestEffort
      .mapMPar(10) {
        case Left(err)  =>
          logger.throwable("error during processing messages.", err)
        case Right(msg) =>
          //TODO handle error here properly
          protocol.onBestEffortMessage
            .tupled(msg)
            .flatMap(send)
            .tapError(logger.throwable("error during processing incoming message.", _))
            .either
      }
      .runDrain
      .fork *>
      protocol.produceMessages.either
        .mapMPar(10) {
          case Left(err)  => logger.throwable("error during processing messages.", err)
          case Right(msg) => send(msg).catchAll(err => logger.throwable("error during send message.", err))
        }
        .runDrain
        .fork
}

object MessageSink {

  val live =
    ZLayer.fromServices[Broadcast, MemberlistTransport, Logger[String], Clock.Service, Nodes, MessageSink](
      (broadcast, transport, logger, clock, nodes) =>
        new MessageSink(
          broadcast,
          transport,
          nodes,
          clock,
          logger
        )
    )

}
