package zio.memberlist.transport

import zio.{Chunk, Has, IO, ZIO, ZManaged}
import zio.memberlist.{NodeAddress, TransportError}
import zio.stream.{Stream, UStream, ZStream}

import java.io.InputStream

trait MemberlistTransport {

  def bindAddress: NodeAddress

  def sendBestEffort(nodeAddress: NodeAddress, payload: Chunk[Byte]): IO[TransportError, Unit]

  def sendReliably(nodeAddress: NodeAddress, payload: Chunk[Byte]): IO[TransportError, Unit]

  def sendReliably(connectionId: ConnectionId, payload: Chunk[Byte]): IO[TransportError, Unit]

  val receiveBestEffort: UStream[Either[TransportError, (NodeAddress, Chunk[Byte])]]

  val receiveReliable: UStream[MemberlistTransport.Connection]

}

object MemberlistTransport {

  case class Connection(
    id: ConnectionId,
    close: ZManaged.Finalizer,
    stream: InputStream
  )

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

  val receiveReliable: ZStream[Has[MemberlistTransport], Nothing, Connection] =
    ZStream.accessStream(_.get.receiveReliable)

}
