//package zio.memberlist.protocols
//
//import upickle.default._
//import zio.memberlist.encoding.MsgPackCodec
//import zio.memberlist.protocols.messages.FailureDetection.{Ack, Alive, Dead, Ping, PingReq, Suspect}
//import zio.memberlist.protocols.messages.Initial.PushPull
//import zio.memberlist.state.{NodeName, NodeState}
//import zio.memberlist.{NodeAddress, SerializationError}
//import zio.{Chunk, IO, memberlist}
//import zio.stream.Stream
//
//object messages {
//
//  sealed trait MemberlistMessage
//
//  object MemberlistMessage {
//    implicit def codec[A: MsgPackCodec]: MsgPackCodec[MemberlistMessage] =
//      MsgPackCodec.tagged[MemberlistMessage][
//        PushPull,
//        Ping,
//        PingReq,
//        Ack,
//        Suspect,
//        Alive,
//        Dead,
//        User[A]
//      ]
//  }
//
//  sealed trait FailureDetection extends MemberlistMessage
//
//  object FailureDetection {
//
//    final case class Ping(
//      seqNo: Long
//    ) extends FailureDetection
//
//    final case class Ack(seqNo: Long) extends FailureDetection
//
//    final case class Nack(seqNo: Long) extends FailureDetection
//
//    final case class PingReq(seqNo: Long, target: NodeName) extends FailureDetection
//
//    final case class Suspect(incarnation: Long, from: NodeName, node: NodeName) extends FailureDetection
//
//    final case class Alive(incarnation: Long, nodeId: NodeName) extends FailureDetection
//
//    final case class Dead(incarnation: Long, from: NodeName, nodeId: NodeName) extends FailureDetection
//
//    implicit val ackCodec: MsgPackCodec[Ack] =
//      MsgPackCodec.fromReadWriter(macroRW[Ack])
//
//    implicit val nackCodec: MsgPackCodec[Nack] =
//      MsgPackCodec.fromReadWriter(macroRW[Nack])
//
//    implicit val pingCodec: MsgPackCodec[Ping] =
//      MsgPackCodec.fromReadWriter(macroRW[Ping])
//
//    implicit val pingReqCodec: MsgPackCodec[PingReq] =
//      MsgPackCodec.fromReadWriter(macroRW[PingReq])
//
//    implicit val suspectCodec: MsgPackCodec[Suspect] =
//      MsgPackCodec.fromReadWriter(macroRW[Suspect])
//
//    implicit val aliveCodec: MsgPackCodec[Alive] =
//      MsgPackCodec.fromReadWriter(macroRW[Alive])
//
//    implicit val deadCodec: MsgPackCodec[Dead] =
//      MsgPackCodec.fromReadWriter(macroRW[Dead])
//
//  }
//
//  sealed trait Initial extends MemberlistMessage
//
//  object Initial {
//
//    case class NodeViewSnapshot(
//      name: NodeName,
//      nodeAddress: NodeAddress,
//      meta: Chunk[Byte],
//      incarnation: Long,
//      state: NodeState
//    )
//
//    case class PushPull(nodes: Chunk[NodeViewSnapshot], join: Boolean) extends Initial
//
//    implicit val nodeViewSnapshotRW: ReadWriter[NodeViewSnapshot] =
//      macroRW[NodeViewSnapshot]
//
//    implicit val localStateCodec: MsgPackCodec[PushPull] =
//      MsgPackCodec.fromReadWriter(macroRW[PushPull])
//
//  }
//
//  final case class User[A](msg: A) extends MemberlistMessage
//
//  object User {
//    implicit def codec[A](implicit ev: MsgPackCodec[A]): MsgPackCodec[User[A]] =
//      new MsgPackCodec[User[A]] {
//        override def fromChunk(chunk: Chunk[Byte]): IO[SerializationError.DeserializationTypeError, User[A]] =
//          ev.fromChunk(chunk).map(User(_))
//
//        override def fromStream(
//          stream: Stream[memberlist.Error, Byte]
//        ): Stream[SerializationError.DeserializationTypeError, User[A]] = ev.fromStream(stream).map(User(_))
//
//        override def toChunk(a: User[A]): IO[SerializationError.SerializationTypeError, Chunk[Byte]] = ev.toChunk(a.msg)
//      }
//  }
//
//  final case class Compound(
//    parts: List[Chunk[Byte]]
//  ) extends MemberlistMessage
//
//  object Compound {
//    implicit val codec: MsgPackCodec[Compound] =
//      MsgPackCodec.fromReadWriter(macroRW[Compound])
//
//    implicit val chunkRW: ReadWriter[Chunk[Byte]] =
//      implicitly[ReadWriter[Array[Byte]]]
//        .bimap[Chunk[Byte]](
//          ch => ch.toArray,
//          arr => Chunk.fromArray(arr)
//        )
//  }
//
//}
