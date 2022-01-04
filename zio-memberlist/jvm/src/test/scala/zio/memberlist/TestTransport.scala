package zio.memberlist

import zio.ZManaged.Finalizer
import zio.memberlist.TransportError.ConnectionNotFound
import zio.memberlist.transport.MemberlistTransport.Connection
import zio.memberlist.transport.{ConnectionId, MemberlistTransport}
import zio.stm.{TMap, TQueue, USTM, ZSTM}
import zio.stream._
import zio.{Chunk, Has, IO, UIO, ULayer}

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

  val allSentBestEffortMessages: USTM[List[(NodeAddress, Chunk[Byte])]] =
    sendBestEffortMessages.takeAll

  val allSentReliableMessages: USTM[List[(NodeAddress, Chunk[Byte])]] =
    sendReliablyMessages.takeAll

  val allSentReliableByConnectionMessages: USTM[List[(ConnectionId, Chunk[Byte])]] =
    sendReliablyByConnectionMessages.takeAll

  final def simulateBestEffortMessage(nodeAddress: NodeAddress, payload: Chunk[Byte]) =
    receiveBestEffortMessages.offer(Right((nodeAddress, payload)))

  final def simulateNewConnection(payload: Chunk[Byte]): UIO[Unit] =
    for {
      id <- ConnectionId.make
      in  = new ByteArrayInputStream(payload.toArray)
      _  <- receiveReliableMessages.offer(Connection(id, Finalizer.noop, in)).commit
    } yield ()

}

object TestTransport {
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
