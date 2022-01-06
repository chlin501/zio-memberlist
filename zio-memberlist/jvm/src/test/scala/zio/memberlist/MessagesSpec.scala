package zio.memberlist

import zio._
import zio.clock.Clock
import zio.console.Console
import zio.logging._
import zio.memberlist.state._
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestEnvironment

object MessagesSpec extends KeeperSpec {

  val logger: ZLayer[Console with Clock, Nothing, Logging] = Logging.console()

  val dependencies =
    (Console.any ++ logger ++ Clock.any ++ TestTransport.live ++ IncarnationSequence.live) >+> Nodes.live(
      NodeName("local-node")
    ) >+> Broadcast.make(64000, 2).toLayer >+> MessageSink.live

  val testNode: Node = Node(NodeName("node-1"), NodeAddress(Chunk(1, 1, 1, 1), 1111), None, NodeState.Alive)

  val spec: Spec[TestEnvironment, TestFailure[Exception], TestSuccess] =
    suite("messages")(
      testM("receiveMessage") {
        for {
          protocol    <- Protocol[TestProtocolMessage].make(
                           {
                             case (sender, TestProtocolMessage.Msg1(i)) =>
                               ZIO.succeed(Message.BestEffortByAddress(sender, TestProtocolMessage.Msg2(i)))
                             case _                                     =>
                               Message.noResponse
                           },
                           (_, _) => Message.noResponse,
                           ZStream.empty
                         )
          _           <- Nodes.addNode(testNode).commit
          _           <- ZIO.accessM[Has[MessageSink]](_.get.process(protocol.binary))
          _           <- TestTransport.simulateBestEffortMessage[TestProtocolMessage](
                           nodeAddress = testNode.addr,
                           payload = TestProtocolMessage.Msg1(100)
                         )
          allMessages <- TestTransport.allSentBestEffortMessages.repeatUntil(_.nonEmpty)
        } yield assert(allMessages)(hasSameElements(List(TestProtocolMessage.Msg2(123), TestProtocolMessage.Msg2(1))))
        //        messages.use { case (testTransport, messages) =>
        //          for {
        //            dl       <- protocol
        //            _        <- Nodes.addNode(testNode).commit
        //            _        <- messages.process(dl.binary)
        //            ping1    <- ByteCodec[PingPong].toChunk(PingPong.Ping(123))
        //            ping2    <- ByteCodec[PingPong].toChunk(PingPong.Ping(321))
        //            _        <- testTransport.incommingMessage(Compound(testNode.name, ping1, List.empty))
        //            _        <- testTransport.incommingMessage(Compound(testNode.name, ping2, List.empty))
        //            messages <- testTransport.outgoingMessages.mapM { case Compound(_, chunk, _) =>
        //                          ByteCodec.decode[PingPong](chunk)
        //                        }.take(2).runCollect
        //          } yield assert(messages)(hasSameElements(List(PingPong.Pong(123), PingPong.Pong(321))))
        //        }
      }
      //      testM("should not exceed size of message") {
      //
      //        val protocol = Protocol[PingPong].make(
      //          {
      //            case Message.BestEffortByName(sender, Ping(i)) =>
      //              ZIO.succeed(
      //                Message.Batch(
      //                  Message.BestEffortByName(sender, Pong(i)),
      //                  Message.Broadcast(Pong(i)),
      //                  List.fill(2000)(Message.Broadcast(Ping(1))): _*
      //                )
      //              )
      //            case _                                         =>
      //              Message.noResponse
      //          },
      //          ZStream.empty
      //        )
      //
      //        messages.use { case (testTransport, messages) =>
      //          for {
      //            dl    <- protocol
      //            _     <- Nodes.addNode(testNode).commit
      //            _     <- messages.process(dl.binary)
      //            ping1 <- ByteCodec[PingPong].toChunk(PingPong.Ping(123))
      //            _     <- testTransport.incommingMessage(Compound(testNode.name, ping1, List.empty))
      //            m     <- testTransport.outgoingMessages.runHead
      //            bytes <- m.fold[IO[SerializationError.SerializationTypeError, Chunk[Byte]]](ZIO.succeedNow(Chunk.empty))(
      //                       ByteCodec[Compound].toChunk(_)
      //                     )
      //          } yield assertTrue(m.map(_.gossip.size).get == 1487) && assertTrue(bytes.size == 62591)
      //        }
      //      }
    ).provideCustomLayer(dependencies)
}
