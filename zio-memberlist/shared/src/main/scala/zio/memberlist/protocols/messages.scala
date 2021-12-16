package zio.memberlist.protocols

import upickle.default._
import zio.memberlist.encoding.ByteCodec
import zio.memberlist.protocols.messages.FailureDetection.{Ack, Alive, Dead, Ping, PingReq, Suspect}
import zio.memberlist.protocols.messages.Initial.PushPull
import zio.memberlist.state.{NodeName, NodeState}
import zio.memberlist.{NodeAddress, SerializationError}
import zio.{Chunk, IO, memberlist}
import zio.stream.Stream

object messages {

  sealed trait MemberlistMessage

  object MemberlistMessage {
    implicit def codec[A: ByteCodec]: ByteCodec[MemberlistMessage] =
      ByteCodec.tagged[MemberlistMessage][
        PushPull,
        Ping,
        PingReq,
        Ack,
        Suspect,
        Alive,
        Dead,
        User[A]
      ]
  }

  sealed trait FailureDetection extends MemberlistMessage

  object FailureDetection {

    final case class Ping(
      seqNo: Long
    ) extends FailureDetection

    final case class Ack(seqNo: Long) extends FailureDetection

    final case class Nack(seqNo: Long) extends FailureDetection

    final case class PingReq(seqNo: Long, target: NodeName) extends FailureDetection

    final case class Suspect(incarnation: Long, from: NodeName, node: NodeName) extends FailureDetection

    final case class Alive(incarnation: Long, nodeId: NodeName) extends FailureDetection

    final case class Dead(incarnation: Long, from: NodeName, nodeId: NodeName) extends FailureDetection

    implicit val ackCodec: ByteCodec[Ack] =
      ByteCodec.fromReadWriter(macroRW[Ack])

    implicit val nackCodec: ByteCodec[Nack] =
      ByteCodec.fromReadWriter(macroRW[Nack])

    implicit val pingCodec: ByteCodec[Ping] =
      ByteCodec.fromReadWriter(macroRW[Ping])

    implicit val pingReqCodec: ByteCodec[PingReq] =
      ByteCodec.fromReadWriter(macroRW[PingReq])

    implicit val suspectCodec: ByteCodec[Suspect] =
      ByteCodec.fromReadWriter(macroRW[Suspect])

    implicit val aliveCodec: ByteCodec[Alive] =
      ByteCodec.fromReadWriter(macroRW[Alive])

    implicit val deadCodec: ByteCodec[Dead] =
      ByteCodec.fromReadWriter(macroRW[Dead])

  }

  sealed trait Initial extends MemberlistMessage

  object Initial {

    case class NodeViewSnapshot(
      name: NodeName,
      nodeAddress: NodeAddress,
      meta: Chunk[Byte],
      incarnation: Long,
      state: NodeState
    )

    case class PushPull(nodes: Chunk[NodeViewSnapshot], join: Boolean) extends Initial

    implicit val nodeViewSnapshotRW: ReadWriter[NodeViewSnapshot] =
      macroRW[NodeViewSnapshot]

    implicit val localStateCodec: ByteCodec[PushPull] =
      ByteCodec.fromReadWriter(macroRW[PushPull])

  }

  final case class User[A](msg: A) extends MemberlistMessage

  object User {
    implicit def codec[A](implicit ev: ByteCodec[A]): ByteCodec[User[A]] =
      new ByteCodec[User[A]] {
        override def fromChunk(chunk: Chunk[Byte]): IO[SerializationError.DeserializationTypeError, User[A]] =
          ev.fromChunk(chunk).map(User(_))

        override def fromStream(
          stream: Stream[memberlist.Error, Byte]
        ): Stream[SerializationError.DeserializationTypeError, User[A]] = ev.fromStream(stream).map(User(_))

        override def toChunk(a: User[A]): IO[SerializationError.SerializationTypeError, Chunk[Byte]] = ev.toChunk(a.msg)
      }
  }

  final case class Compound(
    parts: List[Chunk[Byte]]
  ) extends MemberlistMessage

  object Compound {
    implicit val codec: ByteCodec[Compound] =
      ByteCodec.fromReadWriter(macroRW[Compound])

    implicit val chunkRW: ReadWriter[Chunk[Byte]] =
      implicitly[ReadWriter[Array[Byte]]]
        .bimap[Chunk[Byte]](
          ch => ch.toArray,
          arr => Chunk.fromArray(arr)
        )
  }

}
