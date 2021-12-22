package zio.memberlist

import zio.memberlist.TransportError.ExceptionWrapper
import zio.nio.core.{InetAddress, InetSocketAddress}
import zio.{Chunk, IO, UIO}

import java.net.{InetAddress => JInetAddress}

final case class NodeAddress(addr: Chunk[Byte], port: Int) {

  def socketAddress: IO[TransportError, InetSocketAddress] =
    (for {
      addr <- InetAddress.byAddress(addr)
      sa   <- InetSocketAddress.inetAddress(addr, port)
    } yield sa).mapError(ExceptionWrapper(_))

  override def toString: String = JInetAddress.getByAddress(addr.toArray) + ": " + port
}

object NodeAddress {

  def fromSocketAddress(addr: InetSocketAddress): UIO[NodeAddress] =
    addr.hostString.flatMap(host =>
      InetAddress
        .byName(host)
        .map(inet => NodeAddress(inet.address, addr.port))
        .orDie
    )

  def local(port: Int): UIO[NodeAddress] =
    InetAddress.localHost
      .map(addr => NodeAddress(addr.address, port))
      .orDie

}
