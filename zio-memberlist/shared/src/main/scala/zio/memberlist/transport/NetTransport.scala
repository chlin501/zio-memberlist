package zio.memberlist.transport
import zio.ZManaged.Scope
import zio.config.getConfig
import zio.memberlist.TransportError._
import zio.memberlist.{MemberlistConfig, NodeAddress, TransportError, transport}
import zio.nio.channels.DatagramChannel
import zio.nio.core.{Buffer, InetSocketAddress}
import zio.stm.TMap
import zio.stream.{UStream, ZStream}
import zio.{Chunk, Has, IO, Managed, Queue, Task, ZIO, ZLayer, ZManaged, ZQueue}

import java.lang.{Void => JVoid}
import java.net.{InetAddress => JInetAddress, InetSocketAddress => JInetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{
  Channels,
  AsynchronousServerSocketChannel => JAsynchronousServerSocketChannel,
  AsynchronousSocketChannel => JAsynchronousSocketChannel
}

class NetTransport(
  override val bindAddress: NodeAddress,
  datagramChannel: DatagramChannel,
  connectionCache: TMap[ConnectionId, JAsynchronousSocketChannel],
  mtu: Int,
  connectionScope: Scope,
  connectionQueue: Queue[transport.MemberlistTransport.Connection]
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
          transport.MemberlistTransport.Connection(
            id = id,
            close = close,
            stream = stream
          )
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
          channel.connect(new JInetSocketAddress(JInetAddress.getByAddress(to.addr.toArray), to.port), (), handler)
        )
      )

  override def sendReliably(connectionId: ConnectionId, payload: Chunk[Byte]): IO[TransportError, Unit] =
    connectionCache
      .get(connectionId)
      .commit
      .flatMap {
        case Some(channel) =>
          ZIO.effect {
            channel.write(ByteBuffer.wrap(payload.toArray))
          }.mapError(ExceptionWrapper(_))
        case None          =>
          ZIO.fail(ConnectionNotFound(connectionId))
      }
      .mapError(ExceptionWrapper(_))
      .unit

  override val receiveBestEffort: UStream[Either[TransportError, (NodeAddress, Chunk[Byte])]] =
    ZStream.repeatEffect(
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

  override val receiveReliable: UStream[transport.MemberlistTransport.Connection] =
    ZStream.fromQueue(connectionQueue)
}

object NetTransport {

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
                                     new JInetSocketAddress(
                                       JInetAddress.getByAddress(config.bindAddress.addr.toArray),
                                       config.bindAddress.port
                                     )
                                   )
                                 )
                                 .mapError(BindFailed(addr, _))
                             }
        connectionCache <- TMap.empty[ConnectionId, JAsynchronousSocketChannel].commit.toManaged_
        connectionQueue <- ZQueue.bounded[transport.MemberlistTransport.Connection](100).toManaged(_.shutdown)
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
                               transport.MemberlistTransport.Connection(
                                 id,
                                 close,
                                 stream
                               )
                             )
                           }.forever.forkManaged
      } yield new NetTransport(
        config.bindAddress,
        datagramChannel = datagramChannel,
        connectionCache = connectionCache,
        mtu = config.messageSizeLimit,
        connectionScope = connectionScope,
        connectionQueue = connectionQueue
      )
    )
}
