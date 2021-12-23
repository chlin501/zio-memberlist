package zio.memberlist

import zio.clock.Clock
import zio.logging.{Logging, log}
import zio.memberlist.encoding.MsgPackCodec
import zio.memberlist.protocols.messages.Compound
import zio.memberlist.state.{NodeName, Nodes}
import zio.memberlist.transport.MemberlistTransport
import zio.stream.Take
import zio.{Chunk, Fiber, Has, ZIO}

final class MessageSink(
  local: NodeName,
  localAddress: NodeAddress,
  broadcast: Broadcast,
  transport: MemberlistTransport
) {

  /**
   * Sends message to target.
   */
  def send(msg: Message[Chunk[Byte]]): ZIO[Clock with Logging with Has[Nodes], Error, Unit] =
    msg match {
      case Message.NoResponse                                => ZIO.unit
      case Message.BestEffortByName(nodeName, message)       =>
        for {
          broadcast    <- broadcast.broadcast(message.size)
          withPiggyback = Compound(Chunk.fromIterable(message :: broadcast))
          chunk        <- MsgPackCodec[Compound].encode(withPiggyback)
          nodeAddress  <- Nodes.nodeAddress(nodeName).commit
          _            <- transport.sendBestEffort(nodeAddress, chunk)
        } yield ()
      case Message.BestEffortByAddress(nodeAddress, message) =>
        for {
          chunk <- MsgPackCodec[Compound].encode(Compound(Chunk.single(message)))
          _     <- transport.sendBestEffort(nodeAddress, chunk)
        } yield ()
      case msg: Message.Batch[Chunk[Byte] @unchecked]        =>
        val (broadcast, rest) =
          (msg.first :: msg.second :: msg.rest.toList).partition(_.isInstanceOf[Message.Broadcast[_]])
        ZIO.foreach_(broadcast)(send) *>
          ZIO.foreach_(rest)(send)
      case msg @ Message.Broadcast(_)                        =>
        broadcast.add(msg.asInstanceOf[Message.Broadcast[Chunk[Byte]]])
      case Message.WithTimeout(message, action, timeout)     =>
        send(message) *> action.delay(timeout).flatMap(send).unit
    }

  private def processTake(
    take: Take[Error, Message[Chunk[Byte]]]
  ): ZIO[Clock with Logging with Has[Nodes], Nothing, Unit] =
    take.foldM(
      ZIO.unit,
      log.error("error: ", _),
      ZIO.foreach_(_)(send(_).catchAll(e => log.throwable("error during send", e)))
    )

  def process(
    protocol: Protocol[Chunk[Byte]]
  ): ZIO[Clock with Logging with Has[Nodes], Nothing, Fiber.Runtime[Nothing, Unit]] =
    transport.receiveBestEffort
      .mapMPar(10) {
        case Left(err)  =>
          log.throwable("error during processing messages.", err)
        case Right(msg) =>
          //TODO handle error here properly
          protocol.onMessage.tupled(msg).either
      }
      .runDrain
      .fork *>
      protocol.produceMessages.either
        .mapMPar(10) {
          case Left(err)  => log.throwable("error during processing messages.", err)
          case Right(msg) => send(msg).catchAll(err => log.throwable("error during send message.", err))
        }
        .runDrain
        .fork
}

object MessageSink {

  def apply(
    local: NodeName,
    nodeAddress: NodeAddress,
    broadcast: Broadcast,
    transport: MemberlistTransport
  ): MessageSink =
    new MessageSink(
      local,
      nodeAddress,
      broadcast,
      transport
    )

}
