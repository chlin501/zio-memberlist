package zio.memberlist

import zio.memberlist.TestProtocolMessage._
import zio.memberlist.encoding.MsgPackCodec
import zio.memberlist.transport.ConnectionId
import zio.stream.ZStream
import zio.test.{Spec, TestFailure, TestSuccess, assertTrue}
import zio.{Chunk, IO, ZIO}

object ProtocolSpec extends KeeperSpec {

  val testNode: NodeAddress = NodeAddress(Chunk(0, 0, 0, 0), 0)

  val protocolFirstPart: IO[Error, Protocol[TestProtocolMessage.FirstPart]] =
    Protocol[TestProtocolMessage.FirstPart].make(
      {
        case (sender, Msg1(i)) =>
          ZIO.succeed(Message.BestEffortByAddress(sender, Msg2(i)))
        case _                 => Message.noResponse
      },
      {
        case (id, Msg1(i)) =>
          ZIO.succeed(Message.ReliableByConnection(id, Msg2(i)))
        case _             => Message.noResponse

      },
      ZStream.repeat(Message.BestEffortByAddress(testNode, Msg2(1)))
    )

  val protocolSecondPart: IO[Error, Protocol[TestProtocolMessage.SecondPart]] =
    Protocol[TestProtocolMessage.SecondPart].make(
      { case (sender, Msg3(i)) =>
        ZIO.succeed(Message.BestEffortByAddress(sender, Msg3(i + 100)))
      },
      { case (id, Msg3(i)) =>
        ZIO.succeed(Message.ReliableByConnection(id, Msg3(i)))
      },
      ZStream.repeat(Message.BestEffortByAddress(testNode, Msg3(1)))
    )

  val spec: Spec[Any, TestFailure[Error], TestSuccess] = suite("protocol spec")(
    testM("request response best effort") {
      for {
        protocol <- protocolFirstPart
        response <- protocol.onBestEffortMessage(testNode, Msg1(123))
      } yield assertTrue(response == Message.BestEffortByAddress(testNode, Msg2(123)))
    },
    testM("binary request response best effort") {
      for {
        protocol       <- protocolFirstPart.map(_.binary)
        binaryMessage  <- MsgPackCodec[TestProtocolMessage].encode(Msg1(123))
        responseBinary <- protocol.onBestEffortMessage(testNode, binaryMessage)
        response       <- responseBinary.transformM(MsgPackCodec[TestProtocolMessage].decode)
      } yield assertTrue(response == Message.BestEffortByAddress[TestProtocolMessage](testNode, Msg2(123)))
    },
    testM("request response reliable") {
      for {
        connectionId <- ConnectionId.make
        protocol     <- protocolFirstPart
        response     <- protocol.onReliableMessage(connectionId, Msg1(123))
      } yield assertTrue(response == Message.ReliableByConnection(connectionId, Msg2(123)))
    },
    testM("produce") {
      for {
        protocol <- protocolFirstPart
        response <- protocol.produceMessages.take(10).runCollect
      } yield assertTrue(response.distinct.size == 1)
    },
    testM("compose") {
      for {
        protocol1            <- protocolFirstPart
        protocol2            <- protocolSecondPart
        protocol              = Protocol.compose[TestProtocolMessage](protocol1, protocol2)
        connectionId         <- ConnectionId.make
        onReliableResponse   <- protocol.onReliableMessage(connectionId, TestProtocolMessage.Msg1(1))
        onBestEffortResponse <- protocol.onBestEffortMessage(testNode, TestProtocolMessage.Msg1(1))
        response             <- protocol.produceMessages.take(10).runCollect
      } yield assertTrue(response.distinct.size == 2) &&
        assertTrue(onReliableResponse == Message.ReliableByConnection(connectionId, Msg2(1))) &&
        assertTrue(onBestEffortResponse == Message.BestEffortByAddress(testNode, Msg2(1)))
    }
  )

}
