package zio.memberlist

import zio.ZManaged.Finalizer
import zio.memberlist.TransportError.ConnectionNotFound
import zio.memberlist.encoding.MsgPackCodec
import zio.memberlist.transport.MemberlistTransport.Connection
import zio.memberlist.transport.{ConnectionId, MemberlistTransport}
import zio.stm.{TMap, TQueue, ZSTM}
import zio.stream._
import zio.{Chunk, Has, IO, UIO, ULayer, ZIO}

import java.io.ByteArrayInputStream

class TestTransport(
  sendBestEffortMessages: TQueue[(NodeAddress, Chunk[Byte])],
  sendReliablyMessages: TQueue[(NodeAddress, Chunk[Byte])],
  sendReliablyByConnectionMessages: TQueue[(ConnectionId, Chunk[Byte])],
  connectionCache: TMap[ConnectionId, MemberlistTransport.Connection],
  receiveBestEffortMessages: TQueue[Either[TransportError, (NodeAddress, Chunk[Byte])]],
  receiveReliableMessages: TQueue[MemberlistTransport.Connection]
) extends MemberlistTransport {

  override def bindAddress: NodeAddress = NodeAddress(Chunk(0, 0, 0, 0), 9000)

  override def sendBestEffort(nodeAddress: NodeAddress, payload: Chunk[Byte]): IO[TransportError, Unit] =
    sendBestEffortMessages.offer((nodeAddress, payload)).commit

  override def sendReliably(nodeAddress: NodeAddress, payload: Chunk[Byte]): IO[TransportError, Unit] =
    sendReliablyMessages.offer((nodeAddress, payload)).commit

  override def sendReliably(connectionId: ConnectionId, payload: Chunk[Byte]): IO[TransportError, Unit] =
    connectionCache
      .get(connectionId)
      .flatMap {
        case Some(_) => sendReliablyByConnectionMessages.offer((connectionId, payload))
        case None    => ZSTM.fail(ConnectionNotFound(connectionId))
      }
      .commit

  override val receiveBestEffort: UStream[Either[TransportError, (NodeAddress, Chunk[Byte])]] =
    Stream.fromTQueue(receiveBestEffortMessages)

  override val receiveReliable: UStream[MemberlistTransport.Connection] =
    Stream.fromTQueue(receiveReliableMessages).tap(conn => connectionCache.put(conn.id, conn).commit)

  val allSentBestEffortMessages: UIO[List[(NodeAddress, Chunk[Byte])]] =
    sendBestEffortMessages.takeAll.commit

  val allSentReliableMessages: UIO[List[(NodeAddress, Chunk[Byte])]] =
    sendReliablyMessages.takeAll.commit

  val allSentReliableByConnectionMessages: UIO[List[(ConnectionId, Chunk[Byte])]] =
    sendReliablyByConnectionMessages.takeAll.commit

  final def simulateBestEffortMessage(nodeAddress: NodeAddress, payload: Chunk[Byte]): UIO[Unit] =
    receiveBestEffortMessages.offer(Right((nodeAddress, payload))).commit

  final def simulateNewConnection(payload: Chunk[Byte]): UIO[Unit] =
    for {
      id <- ConnectionId.make
      in  = new ByteArrayInputStream(payload.toArray)
      _  <- receiveReliableMessages.offer(Connection(id, Finalizer.noop, in)).commit
    } yield ()

}

object TestTransport {

  val allSentBestEffortMessages: ZIO[Has[TestTransport], Nothing, List[(NodeAddress, Chunk[Byte])]] =
    ZIO.accessM(_.get.allSentBestEffortMessages)

  val allSentReliableMessages: ZIO[Has[TestTransport], Nothing, List[(NodeAddress, Chunk[Byte])]] =
    ZIO.accessM(_.get.allSentReliableMessages)

  val allSentReliableByConnectionMessages: ZIO[Has[TestTransport], Nothing, List[(ConnectionId, Chunk[Byte])]] =
    ZIO.accessM(_.get.allSentReliableByConnectionMessages)

  final def simulateBestEffortMessage(
    nodeAddress: NodeAddress,
    payload: Chunk[Byte]
  ): ZIO[Has[TestTransport], Nothing, Unit] =
    ZIO.accessM(_.get.simulateBestEffortMessage(nodeAddress, payload))

  final def simulateBestEffortMessage[A: MsgPackCodec](
    nodeAddress: NodeAddress,
    payload: A
  ): ZIO[Has[TestTransport], SerializationError, Unit] =
    MsgPackCodec[A].encode(payload).flatMap(chunk => ZIO.accessM(_.get.simulateBestEffortMessage(nodeAddress, chunk)))

  final def simulateNewConnection(payload: Chunk[Byte]): ZIO[Has[TestTransport], Nothing, Unit] =
    ZIO.accessM(_.get.simulateNewConnection(payload))

  val live: ULayer[Has[TestTransport] with Has[MemberlistTransport]] =
    ZSTM.atomically {
      for {
        sendBestEffortMessages           <- TQueue.unbounded[(NodeAddress, Chunk[Byte])]
        sendReliablyMessages             <- TQueue.unbounded[(NodeAddress, Chunk[Byte])]
        sendReliablyByConnectionMessages <- TQueue.unbounded[(ConnectionId, Chunk[Byte])]
        connectionCache                  <- TMap.empty[ConnectionId, MemberlistTransport.Connection]
        receiveBestEffortMessages        <- TQueue.unbounded[Either[TransportError, (NodeAddress, Chunk[Byte])]]
        receiveReliableMessages          <- TQueue.unbounded[MemberlistTransport.Connection]
      } yield {
        val transport = new TestTransport(
          sendBestEffortMessages,
          sendReliablyMessages,
          sendReliablyByConnectionMessages,
          connectionCache,
          receiveBestEffortMessages,
          receiveReliableMessages
        )
        Has.allOf(transport, transport: MemberlistTransport)
      }
    }.toLayerMany
}
