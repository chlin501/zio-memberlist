package zio.memberlist.protocols

import zio.Chunk
import zio.memberlist.NodeAddress
import zio.memberlist.encoding.MsgPackCodec
import zio.memberlist.state.{NodeName, NodeState}

import java.io.{InputStream, OutputStream}

object messages {

  sealed trait MemberlistMessage

  object MemberlistMessage {
//    implicit def codec[A: MsgPackCodec]: MsgPackCodec[MemberlistMessage] =
//      MsgPackCodec.tagged[MemberlistMessage][
//        Ping,
//        PingReq,
//        Ack,
//        Suspect,
//        Alive,
//        Dead,
//        PushPull,
//        User[A]
//      ]
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

//    implicit val ackCodec: MsgPackCodec[Ack] =
//      MsgPackCodec.fromReadWriter(macroRW[Ack])
//
//    implicit val nackCodec: MsgPackCodec[Nack] =
//      MsgPackCodec.fromReadWriter(macroRW[Nack])

//    implicit val pingCodec: MsgPackCodec[Ping] =
//      MsgPackCodec.fromReadWriter(macroRW[Ping])

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

  }

  sealed trait Initial extends MemberlistMessage

  object Initial {

    case class NodeViewSnapshot(
      name: NodeName,
      nodeAddress: NodeAddress,
      meta: Option[Chunk[Byte]],
      incarnation: Long,
      state: NodeState
    )

    implicit val nodeViewSnapshotCodec =
      MsgPackCodec[
        (
          (String, Array[Byte]),
          (String, Int),
          (String, Array[Byte]),
          (String, String),
          (String, Int),
          (String, NodeState),
          (String, Array[Byte])
        )
      ].bimap(
        { case ((_, addr), (_, incarnation), (_, meta), (_, name), (_, port), (_, state), (_, vst)) =>
          NodeViewSnapshot(
            NodeName(name),
            NodeAddress(Chunk.fromArray(addr), port),
            Option(meta).map(Chunk.fromArray),
            incarnation,
            state
          )
        },
        (view: NodeViewSnapshot) =>
          (
            ("Addr", view.nodeAddress.addr.toArray),
            ("Incarnation", view.incarnation.toInt),
            ("Meta", view.meta.getOrElse(Chunk.empty).toArray),
            ("Name", view.name.name),
            ("Port", view.nodeAddress.port),
            ("State", view.state),
            ("Vst", Chunk[Byte](1, 5, 6, 0, 0).toArray)
          )
      )

    case class PushPull(nodes: Chunk[NodeViewSnapshot], join: Boolean) extends Initial

    implicit val pushPullCodec = new MsgPackCodec[PushPull] {
      override def unsafeDecode(input: InputStream): PushPull = {
        //header
        val ((_, join), (_, nodeNum), (_, user)) =
          MsgPackCodec[((String, Boolean), (String, Int), (String, Int))].unsafeDecode(input)

        val nodes = (1 to nodeNum).map(_ => MsgPackCodec[NodeViewSnapshot].unsafeDecode(input))
        PushPull(Chunk.fromIterable(nodes), join)
      }

      override def unsafeEncode(a: PushPull, output: OutputStream): Unit = ???
    }

  }

  final case class User[A](msg: A) extends MemberlistMessage

  object User {}

  final case class Compound(
    parts: List[Chunk[Byte]]
  ) extends MemberlistMessage

  object Compound {}

}
