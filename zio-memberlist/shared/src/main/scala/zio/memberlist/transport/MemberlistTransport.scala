package zio.memberlist.transport

import zio.{Chunk, Has, IO, ZIO}
import zio.memberlist.{NodeAddress, TransportError}
import zio.stream.{UStream, ZStream, Stream}

trait MemberlistTransport {

  def bindAddress: NodeAddress

  def sendBestEffort(nodeAddress: NodeAddress, payload: Chunk[Byte]): IO[TransportError, Unit]

  def sendReliably(nodeAddress: NodeAddress, payload: Chunk[Byte]): IO[TransportError, Unit]

  def sendReliably(connectionId: ConnectionId, payload: Chunk[Byte]): IO[TransportError, Unit]

  val receiveBestEffort: UStream[Either[TransportError, (NodeAddress, Chunk[Byte])]]

  val receiveReliable: UStream[(ConnectionId, Stream[TransportError, Byte])]

}

object MemberlistTransport {

  def sendBestEffort(
    nodeAddress: NodeAddress,
    payload: Chunk[Byte]
  ): ZIO[Has[MemberlistTransport], TransportError, Unit] =
    ZIO.accessM(_.get.sendBestEffort(nodeAddress: NodeAddress, payload: Chunk[Byte]))

  def sendReliably(
    nodeAddress: NodeAddress,
    payload: Chunk[Byte]
  ): ZIO[Has[MemberlistTransport], TransportError, Unit] =
    ZIO.accessM(_.get.sendReliably(nodeAddress, payload))

  def sendReliably(
    connectionId: ConnectionId,
    payload: Chunk[Byte]
  ): ZIO[Has[MemberlistTransport], TransportError, Unit] =
    ZIO.accessM(_.get.sendReliably(connectionId, payload))

  val receiveBestEffort
    : ZStream[Has[MemberlistTransport], Nothing, Either[TransportError, (NodeAddress, Chunk[Byte])]] =
    ZStream.accessStream(_.get.receiveBestEffort)

  val receiveReliable: ZStream[Has[MemberlistTransport], Nothing, (ConnectionId, Stream[TransportError, Byte])] =
    ZStream.accessStream(_.get.receiveReliable)

}
