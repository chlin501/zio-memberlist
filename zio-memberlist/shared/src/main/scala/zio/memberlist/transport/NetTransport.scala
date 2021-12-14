package zio.memberlist.transport
import zio.ZManaged.Scope
import zio.config.getConfig
import zio.duration._
import zio.memberlist.TransportError._
import zio.memberlist.{MemberlistConfig, NodeAddress, TransportError}
import zio.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, DatagramChannel}
import zio.nio.core.{Buffer, ByteBuffer, InetSocketAddress}
import zio.stm.TMap
import zio.stream.{Stream, UStream, ZStream}
import zio.{Chunk, Has, IO, Queue, ZIO, ZLayer, ZManaged, ZQueue}

import java.io.EOFException

class NetTransport(
  override val bindAddress: NodeAddress,
  datagramChannel: DatagramChannel,
  serverChannel: AsynchronousServerSocketChannel,
  connectionCache: TMap[ConnectionId, AsynchronousSocketChannel],
  mtu: Int,
  bufferSize: Int,
  tcpReadTimeout: Duration,
  connectionScope: Scope,
  connectionQueue: Queue[NetTransport.Connection]
) extends MemberlistTransport {

  override def sendBestEffort(nodeAddress: NodeAddress, payload: Chunk[Byte]): IO[TransportError, Unit] =
    nodeAddress.socketAddress
      .zip(Buffer.byte(payload))
      .flatMap { case (addr, bytes) => datagramChannel.send(bytes, addr) }
      .mapError(ExceptionWrapper(_))
      .unit

  override def sendReliably(nodeAddress: NodeAddress, payload: Chunk[Byte]): IO[TransportError, Unit] =
    connectionScope(
      AsynchronousSocketChannel()
        .zip(ConnectionId.make.toManaged_)
        .mapError(ExceptionWrapper(_))
        .tapM { case (channel, uuid) => connectionCache.put(uuid, channel).commit }
    ).flatMap { case (close, (channel, id)) =>
      nodeAddress.socketAddress.flatMap(channel.connect(_)) *>
        channel.writeChunk(payload) *>
        Buffer
          .byte(bufferSize)
          .map(buffer => NetTransport.channelStream(channel, buffer, tcpReadTimeout))
          .flatMap(byteStream => connectionQueue.offer(NetTransport.Connection(id, close, byteStream)))
    }
      .mapError(ExceptionWrapper(_))
      .unit

  override def sendReliably(connectionId: ConnectionId, payload: Chunk[Byte]): IO[TransportError, Unit] =
    connectionCache
      .get(connectionId)
      .commit
      .flatMap {
        case Some(channel) =>
          channel.writeChunk(payload)
        case None          =>
          ZIO.fail(ConnectionNotFound(connectionId))
      }
      .mapError(ExceptionWrapper(_))
      .unit

  override val receiveBestEffort: UStream[Either[TransportError, (NodeAddress, Chunk[Byte])]] =
    ZStream.fromEffect(
      Buffer
        .byte(mtu)
        .flatMap(buffer =>
          datagramChannel
            .receive(buffer)
            .mapError(ExceptionWrapper(_))
            .zipLeft(buffer.flip)
            .flatMap {
              case Some(remoteAddr: InetSocketAddress) =>
                NodeAddress.fromSocketAddress(remoteAddr) <*> buffer.getChunk()
              case _                                   =>
                ZIO.dieMessage("address for connection unavailable")
            }
        )
        .either
    )

  override val receiveReliable: UStream[(ConnectionId, Stream[TransportError, Byte])] =
    ZStream.fromQueue(connectionQueue).map(conn => (conn.id, conn.stream))
}

object NetTransport {

  private case class Connection(
    id: ConnectionId,
    close: ZManaged.Finalizer,
    stream: Stream[TransportError, Byte]
  )

  val live: ZLayer[Has[MemberlistConfig], TransportError, Has[MemberlistTransport]] =
    ZLayer.fromManaged(
      for {
        config          <- getConfig[MemberlistConfig].toManaged_
        addr            <- config.bindAddress.socketAddress.toManaged_
        datagramChannel <- DatagramChannel
                             .bind(Some(addr))
                             .mapError(BindFailed(addr, _))
        serverChannel   <- AsynchronousServerSocketChannel()
                             .mapError(ExceptionWrapper(_))
                             .tapM { server =>
                               server.bind(Some(addr)).mapError(BindFailed(addr, _))
                             }
        connectionCache <- TMap.empty[ConnectionId, AsynchronousSocketChannel].commit.toManaged_
        connectionQueue <- ZQueue.bounded[Connection](100).toManaged(_.shutdown)
        connectionScope <- ZManaged.scope
        _               <- connectionScope(
                             serverChannel.accept
                               .mapError(ExceptionWrapper(_))
                               .zip(ConnectionId.make.toManaged_)
                               .tapM { case (channel, uuid) => connectionCache.put(uuid, channel).commit }
                               .zip(Buffer.byte(1024 * 1024 * 2).toManaged_)
                               .mapBoth(
                                 ExceptionWrapper(_),
                                 { case ((channel, uuid), buffer) => (uuid, channelStream(channel, buffer, 5.seconds)) }
                               )
                           ).flatMap { case (close, (id, stream)) =>
                             connectionQueue.offer(Connection(id, close, stream))
                           }.forever.fork.toManaged(_.interrupt)
      } yield new NetTransport(
        config.bindAddress,
        datagramChannel = datagramChannel,
        serverChannel = serverChannel,
        connectionCache = connectionCache,
        mtu = config.messageSizeLimit,
        bufferSize = 1024 * 1024 * 2,
        tcpReadTimeout = 5.seconds,
        connectionScope = connectionScope,
        connectionQueue = connectionQueue
      )
    )

  private def channelStream(channel: AsynchronousSocketChannel, buffer: ByteBuffer, tcpReadTimeout: Duration) =
    //    to musi byc queue dla kazdego polaczenia i fork ktory robi read i z tego stream
    ZStream.repeatEffectChunkOption[Any, TransportError, Byte](
      channel
        .read(buffer, tcpReadTimeout)
        .zipLeft(buffer.flip)
        .mapError(err => Some(ExceptionWrapper(err)))
        .flatMap(bytes =>
          if (bytes == -1)
            ZIO.fail(None)
          else
            buffer.getChunk().zipLeft(buffer.clear)
        )
        .catchSome {
          //case Some(ExceptionWrapper(_: InterruptedByTimeoutException)) => ZIO.fail(None)
          case Some(ExceptionWrapper(_: EOFException)) => ZIO.fail(None)
        }
    )

}
