//package zio.memberlist
//
//import zio.clock.Clock
//import zio.logging.{Logging, log}
//import zio.memberlist.encoding.MsgPackCodec
//import zio.memberlist.protocols.messages.Compound
//import zio.memberlist.state.{NodeName, Nodes}
//import zio.memberlist.transport.{ChunkConnection, ConnectionLessTransport, Transport}
//import zio.stm.TMap
//import zio.stream.{Take, ZStream}
//import zio.{Cause, Chunk, Exit, Fiber, Has, IO, Queue, ZIO, ZManaged}
//
//class MessageSink(
//  val local: NodeName,
//  commands: Queue[Take[Error, (NodeAddress, Chunk[Byte])]],
//  broadcast: Broadcast,
//  sendBestEffort: (NodeAddress, Chunk[Byte]) => IO[TransportError, Unit],
//  sendReliable: (NodeAddress, Chunk[Byte]) => IO[TransportError, Unit]
//) {
//
//  /**
//   * Sends message to target.
//   */
//  def send(msg: Message[Chunk[Byte]]): ZIO[Clock with Logging with Has[Nodes], Error, Unit] =
//    msg match {
//      case Message.NoResponse                                => ZIO.unit
//      case Message.BestEffortByName(nodeName, message)       =>
//        for {
//          broadcast    <- broadcast.broadcast(message.size)
//          withPiggyback = Compound(message :: broadcast)
//          chunk        <- MsgPackCodec[Compound].toChunk(withPiggyback)
//          nodeAddress  <- Nodes.nodeAddress(nodeName).commit
//          _            <- sendBestEffort(nodeAddress, chunk)
//        } yield ()
//      case Message.BestEffortByAddress(nodeAddress, message) =>
//        for {
//          chunk <- MsgPackCodec[Compound].toChunk(Compound(message :: Nil))
//          _     <- sendBestEffort(nodeAddress, chunk)
//        } yield ()
//      case msg: Message.Batch[Chunk[Byte] @unchecked]        =>
//        val (broadcast, rest) =
//          (msg.first :: msg.second :: msg.rest.toList).partition(_.isInstanceOf[Message.Broadcast[_]])
//        ZIO.foreach_(broadcast)(send) *>
//          ZIO.foreach_(rest)(send)
//      case msg @ Message.Broadcast(_)                        =>
//        broadcast.add(msg.asInstanceOf[Message.Broadcast[Chunk[Byte]]])
//      case Message.WithTimeout(message, action, timeout)     =>
//        send(message) *> action.delay(timeout).flatMap(send).unit
//    }
//
//  private def processTake(
//    take: Take[Error, Message[Chunk[Byte]]]
//  ): ZIO[Clock with Logging with Has[Nodes], Nothing, Unit] =
//    take.foldM(
//      ZIO.unit,
//      log.error("error: ", _),
//      ZIO.foreach_(_)(send(_).catchAll(e => log.throwable("error during send", e)))
//    )
//
//  def process(
//    protocol: Protocol[Chunk[Byte]]
//  ): ZIO[Clock with Logging with Has[Nodes], Nothing, Fiber.Runtime[Nothing, Unit]] =
//    ZStream
//      .fromQueue(commands)
//      .mapMPar(10) {
//        case Take(Exit.Failure(cause)) =>
//          log.error("error during processing messages.", cause)
//        case Take(Exit.Success(msgs))  =>
//          ZIO.foreach(msgs) { msg =>
//            Take.fromEffect(protocol.onMessage.tupled(msg)).flatMap(processTake(_))
//
//          }
//      }
//      .runDrain
//      .fork *>
//      MessageSink
//        .recoverErrors(protocol.produceMessages)
//        .mapMPar(10)(processTake)
//        .runDrain
//        .fork
//}
//
//object MessageSink {
//
//  private def recoverErrors[R, E, A](stream: ZStream[R, E, A]): ZStream[R with Logging, Nothing, Take[E, A]] =
//    stream.either.mapM(
//      _.fold(
//        e => log.error("error during sending", Cause.fail(e)).as(Take.halt(Cause.fail(e))),
//        v => ZIO.succeedNow(Take.single(v))
//      )
//    )
//
//  def make(
//    local: NodeName,
//    nodeAddress: NodeAddress,
//    broadcast: Broadcast,
//    udpTransport: ConnectionLessTransport,
//    tcpTransport: Transport
//  ): ZManaged[Logging, TransportError, MessageSink] =
//    for {
//      messageQueue    <- Queue
//                           .bounded[Take[Error, (NodeAddress, Chunk[Byte])]](1000)
//                           .toManaged(_.shutdown)
//      connectionCache <- TMap.empty[NodeAddress, ChunkConnection].commit.toManaged_
//      bindUdp         <- ConnectionHandler.bind(nodeAddress, udpTransport, messageQueue)
//      //bindTcp <- tcpTransport.bind(nodeAddress).mapM(conn => conn.)
//    } yield new MessageSink(
//      local,
//      messageQueue,
//      broadcast,
//      (addr, chunk) => addr.socketAddress.flatMap(bindUdp.send(_, chunk)),
//      tcpTransport.send
//    )
//
//}
