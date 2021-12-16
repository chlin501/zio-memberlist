package zio.memberlist.transport
import zio.ZManaged.Scope
import zio.config.getConfig
import zio.duration._
import zio.memberlist.TransportError._
import zio.memberlist.{MemberlistConfig, NodeAddress, TransportError}
import zio.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, DatagramChannel}
import zio.nio.core.{Buffer, ByteBuffer, InetSocketAddress}
import zio.stm.TMap
import zio.stream.{UStream, ZStream}
import zio.{Chunk, Has, IO, Managed, Queue, Task, ZIO, ZLayer, ZManaged, ZQueue}

import java.io.{EOFException, InputStream}
import java.lang.{Void => JVoid}
import java.net.{InetSocketAddress => JInetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{
  Channels,
  InterruptedByTimeoutException,
  AsynchronousServerSocketChannel => JAsynchronousServerSocketChannel,
  AsynchronousSocketChannel => JAsynchronousSocketChannel
}

class NetTransport(
  override val bindAddress: NodeAddress,
  datagramChannel: DatagramChannel,
  serverChannel: JAsynchronousServerSocketChannel,
  connectionCache: TMap[ConnectionId, JAsynchronousSocketChannel],
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
      createConnection(nodeAddress)
        .zip(ConnectionId.make.toManaged_)
        .mapError(ExceptionWrapper(_))
        .tapM { case (channel, uuid) => connectionCache.put(uuid, channel).commit }
    ).flatMap { case (close, (channel, id)) =>
      ZIO.effect {
        channel.write(ByteBuffer.wrap(payload.toArray))
        Channels.newInputStream(channel)
      }.flatMap(stream =>
        connectionQueue.offer(
          NetTransport.Connection(id, close, ZManaged.makeEffect(stream)(_.close()).mapError(ExceptionWrapper(_)))
        )
      )
    }
      .mapError(ExceptionWrapper(_))
      .unit

  private def createConnection(to: NodeAddress): ZManaged[Any, Throwable, JAsynchronousSocketChannel] =
    ZManaged
      .makeEffect(JAsynchronousSocketChannel.open())(_.close())
      .tapM(channel =>
        Task.effectAsyncWithCompletionHandler[JVoid](handler =>
          channel.connect(JInetSocketAddress.createUnresolved(to.hostName, to.port), (), handler)
        )
      )

  override def sendReliably(connectionId: ConnectionId, payload: Chunk[Byte]): IO[TransportError, Unit] =
    connectionCache
      .get(connectionId)
      .commit
      .flatMap {
        case Some(channel) =>
          ZIO.effect(channel.write(ByteBuffer.wrap(payload.toArray))).mapError(ExceptionWrapper(_))
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

  override val receiveReliable: UStream[(ConnectionId, ZManaged[Any, TransportError, InputStream])] =
    ZStream.fromQueue(connectionQueue).map(conn => (conn.id, conn.stream))
}

object NetTransport {

  private case class Connection(
    id: ConnectionId,
    close: ZManaged.Finalizer,
    stream: ZManaged[Any, TransportError, InputStream]
  )

  val live: ZLayer[Has[MemberlistConfig], TransportError, Has[MemberlistTransport]] =
    ZLayer.fromManaged(
      for {
        config          <- getConfig[MemberlistConfig].toManaged_
        addr            <- config.bindAddress.socketAddress.toManaged_
        datagramChannel <- DatagramChannel
                             .bind(Some(addr))
                             .mapError(BindFailed(addr, _))
        serverChannel   <- ZManaged
                             .makeEffect(JAsynchronousServerSocketChannel.open())(_.close())
                             .mapError(ExceptionWrapper(_))
                             .tapM { server =>
                               ZIO
                                 .effect(
                                   server.bind(
                                     new JInetSocketAddress(config.bindAddress.hostName, config.bindAddress.port)
                                   )
                                 )
                                 .mapError(BindFailed(addr, _))
                             }
        connectionCache <- TMap.empty[ConnectionId, JAsynchronousSocketChannel].commit.toManaged_
        connectionQueue <- ZQueue.bounded[Connection](100).toManaged(_.shutdown)
        connectionScope <- ZManaged.scope
        _               <- connectionScope(
                             Managed
                               .makeInterruptible(
                                 Task.effectAsyncWithCompletionHandler[JAsynchronousSocketChannel](handler =>
                                   serverChannel.accept((), handler)
                                 )
                               )(channel => ZIO.effect(channel.close()).orDie)
                               .mapError(ExceptionWrapper(_))
                               .zip(ConnectionId.make.toManaged_)
                               .tapM { case (channel, uuid) => connectionCache.put(uuid, channel).commit }
                               .mapBoth(
                                 ExceptionWrapper(_),
                                 { case (channel, uuid) => (uuid, Channels.newInputStream(channel)) }
                               )
                           ).flatMap { case (close, (id, stream)) =>
                             connectionQueue.offer(
                               Connection(id, close, ZManaged.makeEffect(stream)(_.close).mapError(ExceptionWrapper(_)))
                             )
                           }.forever.forkManaged
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
}
