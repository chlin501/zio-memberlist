package zio.memberlist

import upickle.default._
import zio.memberlist.TransportError._
import zio.memberlist.encoding.MsgPackCodec
import zio.nio.core.{InetAddress, InetSocketAddress}
import zio.{Chunk, IO, UIO}

import java.io.{InputStream, OutputStream}

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

  implicit val byteCodec: MsgPackCodec[NodeAddress] =
    new MsgPackCodec[NodeAddress] {
      override def unsafeDecode(input: InputStream): NodeAddress = ???

      override def unsafeEncode(a: NodeAddress, output: OutputStream): Unit = ???
    }

}
