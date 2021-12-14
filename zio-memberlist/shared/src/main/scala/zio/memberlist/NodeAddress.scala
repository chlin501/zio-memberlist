package zio.memberlist

import upickle.default._
import zio.memberlist.TransportError._
import zio.memberlist.encoding.ByteCodec
import zio.nio.core.{InetAddress, InetSocketAddress}
import zio.{Chunk, IO, UIO}

final case class NodeAddress(hostName: String, port: Int) {

  def socketAddress: IO[TransportError, InetSocketAddress] =
    (for {
      addr <- InetAddress.byName(hostName)
      sa   <- InetSocketAddress.inetAddress(addr, port)
    } yield sa).mapError(ExceptionWrapper(_))

  override def toString: String = hostName + ": " + port
}

object NodeAddress {

  def fromSocketAddress(addr: InetSocketAddress): UIO[NodeAddress] =
    addr.hostString.flatMap(host =>
      InetAddress
        .byName(host)
        .map(inet => NodeAddress(inet.hostName, addr.port))
        .orDie
    )

  def local(port: Int): UIO[NodeAddress] =
    InetAddress.localHost
      .map(addr => NodeAddress(addr.hostName, port))
      .orDie

  implicit val nodeAddressRw: ReadWriter[NodeAddress] = macroRW[NodeAddress]

  implicit val byteCodec: ByteCodec[NodeAddress] =
    ByteCodec.fromReadWriter(nodeAddressRw)

}
