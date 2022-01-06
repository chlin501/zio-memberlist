package zio.memberlist

import zio.memberlist.encoding.MsgPackCodec
import zio.memberlist.transport.ConnectionId
import zio.stream._
import zio.{Chunk, IO, ZIO}

import scala.reflect.ClassTag

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
    transform(MsgPackCodec[M].encode, MsgPackCodec[M].decode)

  final def transform[M1](to: M => IO[Error, M1], from: M1 => IO[Error, M]): Protocol[M1] =
    new Protocol[M1] {

      override val onBestEffortMessage: (NodeAddress, M1) => IO[Error, Message[M1]] =
        (addr, msg) =>
          from(msg)
            .flatMap(decoded => self.onBestEffortMessage(addr, decoded))
            .flatMap(_.transformM(to))

      override val produceMessages: Stream[Error, Message[M1]] =
        self.produceMessages.mapM(_.transformM(to))

      /**
       * Handler for incoming messages.
       */
      override def onReliableMessage: (ConnectionId, M1) => IO[Error, Message[M1]] = (id, msg) =>
        from(msg)
          .flatMap(decoded => self.onReliableMessage(id, decoded))
          .flatMap(_.transformM(to))
    }

  /**
   * Handler for incoming messages.
   */
  def onBestEffortMessage: (NodeAddress, M) => IO[Error, Message[M]]

  /**
   * Handler for incoming messages.
   */
  def onReliableMessage: (ConnectionId, M) => IO[Error, Message[M]]

  /**
   * Stream of outgoing messages.
   */
  val produceMessages: Stream[Error, Message[M]]

}

object Protocol {

  final class ProtocolComposePartiallyApplied[A] {
    def apply[A1 <: A: ClassTag, A2 <: A: ClassTag](
      a1: Protocol[A1],
      a2: Protocol[A2]
    ) = new Protocol[A] {

      override val onBestEffortMessage: (NodeAddress, A) => IO[Error, Message[A]] = {
        case (nodeAddress, message: A1) =>
          a1.onBestEffortMessage(nodeAddress, message)
        case (nodeAddress, message: A2) =>
          a2.onBestEffortMessage(nodeAddress, message)
        case (nodeAddress, msg)         =>
          ZIO.fail(ProtocolError.UnhandledBestEffortMessage(nodeAddress, msg))
      }

      override val produceMessages: Stream[Error, Message[A]] = {
        val allStreams: List[Stream[Error, Message[A]]] =
          a1.asInstanceOf[Protocol[A]].produceMessages :: a2
            .asInstanceOf[Protocol[A]]
            .produceMessages :: Nil

        ZStream.mergeAllUnbounded()(allStreams: _*)
      }

      /**
       * Handler for incoming messages.
       */
      override def onReliableMessage: (ConnectionId, A) => IO[Error, Message[A]] = {
        case (connectionId, message: A1) => a1.onReliableMessage(connectionId, message)
        case (connectionId, message: A2) => a2.onReliableMessage(connectionId, message)
        case (connectionId, message)     => ZIO.fail(ProtocolError.UnhandledReliableMessage(connectionId, message))
      }
    }
    def apply[A1 <: A: ClassTag, A2 <: A: ClassTag, A3 <: A: ClassTag](
      a1: Protocol[A1],
      a2: Protocol[A2],
      a3: Protocol[A3]
    ) = new Protocol[A] {

      override val onBestEffortMessage: (NodeAddress, A) => IO[Error, Message[A]] = {
        case (nodeAddress, message: A1) =>
          a1.onBestEffortMessage(nodeAddress, message)
        case (nodeAddress, message: A2) =>
          a2.onBestEffortMessage(nodeAddress, message)
        case (nodeAddress, message: A3) =>
          a3.onBestEffortMessage(nodeAddress, message)
        case (nodeAddress, msg)         =>
          ZIO.fail(ProtocolError.UnhandledBestEffortMessage(nodeAddress, msg))

      }

      override val produceMessages: Stream[Error, Message[A]] = {
        val allStreams: List[Stream[Error, Message[A]]] =
          a1.asInstanceOf[Protocol[A]].produceMessages :: a2
            .asInstanceOf[Protocol[A]]
            .produceMessages :: a3.asInstanceOf[Protocol[A]].produceMessages :: Nil

        ZStream.mergeAllUnbounded()(allStreams: _*)
      }

      /**
       * Handler for incoming messages.
       */
      override def onReliableMessage: (ConnectionId, A) => IO[Error, Message[A]] = {
        case (connectionId, message: A1) => a1.onReliableMessage(connectionId, message)
        case (connectionId, message: A2) => a2.onReliableMessage(connectionId, message)
        case (connectionId, message: A3) => a3.onReliableMessage(connectionId, message)
        case (connectionId, message)     => ZIO.fail(ProtocolError.UnhandledReliableMessage(connectionId, message))
      }
    }
  }

  def compose[A] = new ProtocolComposePartiallyApplied[A]

  class ProtocolBuilder[M] {

    def make[R, R1, R2](
      bestEffort: (NodeAddress, M) => ZIO[R, Error, Message[M]],
      reliable: (ConnectionId, M) => ZIO[R2, Error, Message[M]],
      out: zio.stream.ZStream[R1, Error, Message[M]]
    ): ZIO[R with R1 with R2, Error, Protocol[M]] =
      ZIO.access[R with R1 with R2](env =>
        new Protocol[M] {

          override val onBestEffortMessage: (NodeAddress, M) => IO[Error, Message[M]] =
            (addr, msg) => bestEffort(addr, msg).provide(env)

          override val produceMessages: Stream[Error, Message[M]] =
            out.provide(env)

          /**
           * Handler for incoming messages.
           */
          override def onReliableMessage: (ConnectionId, M) => IO[Error, Message[M]] =
            (id, msg) => reliable(id, msg).provide(env)
        }
      )
  }

  def apply[M]: ProtocolBuilder[M] =
    new ProtocolBuilder[M]

}
