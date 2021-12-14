//package zio.memberlist
//
//import zio._
//import zio.memberlist.encoding.ByteCodec
//import zio.memberlist.protocols.messages.Compound
//import zio.memberlist.transport.{Bind, Channel, ConnectionLessTransport}
//import zio.nio.core.SocketAddress
//import zio.stream._
//
//class TestTransport(in: Queue[(NodeAddress, Chunk[Byte])], out: Queue[(NodeAddress, Chunk[Byte])])
//    extends ConnectionLessTransport {
//
//  override def bind(
//    localAddr: SocketAddress
//  )(connectionHandler: Channel => zio.UIO[Unit]): zio.Managed[TransportError, Bind] =
//    ZStream
//      .fromQueue(in)
//      .mapM(ByteCodec[Compound].toChunk)
//      .foreach { chunk =>
//        val size          = chunk.size
//        var chunkWithSize = Chunk((size >>> 24).toByte, (size >>> 16).toByte, (size >>> 8).toByte, size.toByte) ++ chunk
//        val read          = (size: Int) => {
//          val bytes = chunkWithSize.take(size)
//          chunkWithSize = chunkWithSize.drop(size)
//          ZIO.succeedNow(bytes)
//        }
//        connectionHandler(new Channel(read, _ => ZIO.unit, ZIO.succeed(true), ZIO.unit))
//
//      }
//      .fork
//      .as(
//        new Bind(
//          in.isShutdown,
//          in.shutdown,
//          ZIO.succeed(localAddr),
//          (_, chunk) =>
//            ByteCodec[Compound]
//              .fromChunk(chunk)
//              .flatMap(out.offer(_))
//              .ignore
//        )
//      )
//      .toManaged(_.close.ignore)
//
//  def incommingMessage(msg: Compound): UIO[Boolean] = in.offer(msg)
//  def outgoingMessages: Stream[Nothing, Compound]   = ZStream.fromQueue(out)
//
//}
//
//object TestTransport {
//
//  def make: UManaged[TestTransport] =
//    for {
//      in  <- Queue.unbounded[Compound].toManaged(_.shutdown)
//      out <- Queue.unbounded[Compound].toManaged(_.shutdown)
//    } yield new TestTransport(in, out)
//
//}
