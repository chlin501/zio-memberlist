package zio.memberlist

import zio.logging._
import zio.memberlist.encoding.MsgPackCodec
import zio.stream.{ZStream, _}
import zio.{Chunk, IO, ZIO}

import scala.reflect.{ClassTag, classTag}

/**
 * Protocol represents message flow.
 * @tparam M - type of messages handles by this protocol
 */
trait Protocol[M] {
  self =>

  /**
   * Converts this protocol to Chunk[Byte] protocol. This helps with composing multi protocols together.
   *
   * @param codec - TaggedCodec that handles serialization to Chunk[Byte]
   * @return - Protocol that operates on Chunk[Byte]
   */
  final def binary(implicit codec: MsgPackCodec[M]): Protocol[Chunk[Byte]] =
    new Protocol[Chunk[Byte]] {

      override val onMessage: (NodeAddress, Chunk[Byte]) => IO[Error, Message[Chunk[Byte]]] =
        (addr, msg) =>
          MsgPackCodec[M]
            .decode(msg)
            .flatMap(decoded => self.onMessage(addr, decoded))
            .flatMap(_.transformM(MsgPackCodec[M].encode))

      override val produceMessages: Stream[Error, Message[Chunk[Byte]]] =
        self.produceMessages.mapM(_.transformM(MsgPackCodec[M].encode))
    }

  /**
   * Adds logging to each received and sent message.
   */
  val debug: ZIO[Logging, Error, Protocol[M]] =
    ZIO.access[Logging] { env =>
      new Protocol[M] {
        override def onMessage: (NodeAddress, M) => IO[Error, Message[M]] =
          (addr, msg) =>
            env.get.log(LogLevel.Trace)("Receive [" + msg + "]") *>
              self
                .onMessage(addr, msg)
                .tap(msg => env.get.log(LogLevel.Trace)("Replied with [" + msg + "]"))

        override val produceMessages: Stream[Error, Message[M]] =
          self.produceMessages.tap { msg =>
            env.get.log(LogLevel.Trace)("Sending [" + msg + "]")
          }
      }
    }

  /**
   * Handler for incomming messages.
   */
  def onMessage: (NodeAddress, M) => IO[Error, Message[M]]

  /**
   * Stream of outgoing messages.
   */
  val produceMessages: zio.stream.Stream[Error, Message[M]]

}

object Protocol {

  final class ProtocolComposePartiallyApplied[A] {
    def apply[A1 <: A: ClassTag, A2 <: A: ClassTag, A3 <: A: ClassTag](
      a1: Protocol[A1],
      a2: Protocol[A2],
      a3: Protocol[A3]
    ) = new Protocol[A] {

      override val onMessage: (NodeAddress, A) => IO[Error, Message[A]] = {
        case msg: (NodeAddress @unchecked, A1 @unchecked) if classTag[A1].runtimeClass.isInstance(msg) =>
          a1.onMessage.tupled(msg)
        case msg: (NodeAddress @unchecked, A2 @unchecked) if classTag[A2].runtimeClass.isInstance(msg) =>
          a2.onMessage.tupled(msg)
        case msg: (NodeAddress @unchecked, A3 @unchecked) if classTag[A3].runtimeClass.isInstance(msg) =>
          a3.onMessage.tupled(msg)
        case _                                                                                         =>
          Message.noResponse

      }

      override val produceMessages: Stream[Error, Message[A]] = {
        val allStreams: List[Stream[Error, Message[A]]] =
          a1.asInstanceOf[Protocol[A]].produceMessages :: a2
            .asInstanceOf[Protocol[A]]
            .produceMessages :: a3.asInstanceOf[Protocol[A]].produceMessages :: Nil

        ZStream.mergeAllUnbounded()(allStreams: _*)
      }

    }
  }

  def compose[A] = new ProtocolComposePartiallyApplied[A]

  class ProtocolBuilder[M] {

    def make[R, R1](
      in: (NodeAddress, M) => ZIO[R, Error, Message[M]],
      out: zio.stream.ZStream[R1, Error, Message[M]]
    ): ZIO[R with R1, Error, Protocol[M]] =
      ZIO.access[R with R1](env =>
        new Protocol[M] {

          override val onMessage: (NodeAddress, M) => IO[Error, Message[M]] =
            (addr, msg) => in(addr, msg).provide(env)

          override val produceMessages: Stream[Error, Message[M]] =
            out.provide(env)
        }
      )
  }

  def apply[M]: ProtocolBuilder[M] =
    new ProtocolBuilder[M]

}
