package zio.memberlist.protocols

import zio.Chunk
import zio.memberlist.NodeAddress
import zio.memberlist.SerializationError.DeserializationTypeError
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
      seqNo: Long,
      targetNode: NodeName,
      sourceAddress: NodeAddress,
      sourceNode: NodeName
    ) extends FailureDetection

    final case class Ack(seqNo: Long, payload: Chunk[Byte]) extends FailureDetection

    final case class Nack(seqNo: Long) extends FailureDetection

    final case class PingReq(seqNo: Long, target: NodeName) extends FailureDetection

    final case class Suspect(incarnation: Long, from: NodeName, node: NodeName) extends FailureDetection

    final case class Alive(incarnation: Long, nodeId: NodeName) extends FailureDetection

    final case class Dead(incarnation: Long, from: NodeName, nodeId: NodeName) extends FailureDetection

    implicit val ackCodec: MsgPackCodec[Ack] =
      MsgPackCodec[
        (
          (String, Chunk[Byte]),
          (String, Int)
        )
      ].bimap(
        { case ((_, paylod), (_, seqNo)) =>
          Ack(seqNo, paylod)
        },
        (msg: Ack) => (("Payload", msg.payload), ("SeqNo", msg.seqNo.toInt))
      )
//
//    implicit val nackCodec: MsgPackCodec[Nack] =
//      MsgPackCodec.fromReadWriter(macroRW[Nack])

    implicit val pingCodec: MsgPackCodec[Ping] =
      MsgPackCodec[
        (
          (String, String),
          (String, Int),
          (String, Chunk[Byte]),
          (String, String),
          (String, Int)
        )
      ].bimap(
        { case ((_, nodeName), (_, seqNo), (_, sourceAddr), (_, sourceName), (_, sourcePort)) =>
          Ping(seqNo, NodeName(nodeName), NodeAddress(sourceAddr, sourcePort), NodeName(sourceName))
        },
        (msg: Ping) =>
          (
            ("Node", msg.targetNode.name),
            ("SeqNo", msg.seqNo.toInt),
            ("SourceAddr", msg.sourceAddress.addr),
            ("SourceNode", msg.sourceNode.name),
            ("SourcePort", msg.sourceAddress.port)
          )
      )

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
          (String, Chunk[Byte]),
          (String, Int),
          (String, Chunk[Byte]),
          (String, String),
          (String, Int),
          (String, NodeState),
          (String, Chunk[Byte])
        )
      ].bimap(
        { case ((_, addr), (_, incarnation), (_, meta), (_, name), (_, port), (_, state), (_, vsn)) =>
          NodeViewSnapshot(
            NodeName(name),
            NodeAddress(addr, port),
            Option(meta),
            incarnation,
            state
          )
        },
        (view: NodeViewSnapshot) =>
          (
            ("Addr", view.nodeAddress.addr),
            ("Incarnation", view.incarnation.toInt),
            ("Meta", view.meta.orNull),
            ("Name", view.name.name),
            ("Port", view.nodeAddress.port),
            ("State", view.state),
            ("Vsn", Chunk[Byte](1, 5, 4, 0, 0, 0))
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

      override def unsafeEncode(a: PushPull, output: OutputStream): Unit = {
        MsgPackCodec[((String, Boolean), (String, Int), (String, Int))]
          .unsafeEncode((("Join", a.join), ("Nodes", a.nodes.size), ("UserStateLen", 0)), output)

        a.nodes.foreach(view => MsgPackCodec[NodeViewSnapshot].unsafeEncode(view, output))
      }
    }

  }

  final case class User[A](msg: A) extends MemberlistMessage

  object User {}

  final case class Compound(
    parts: Chunk[Chunk[Byte]]
  ) extends MemberlistMessage

  object Compound {

    implicit val codec = new MsgPackCodec[Compound] {
      override def unsafeDecode(input: InputStream): Compound = {
        //This is not MsgPack format this is custom serialization from go memberlist
        val allBytes = Chunk.fromArray(input.readAllBytes())
        if (allBytes.size < 1) {
          throw new DeserializationTypeError("missing compound length byte")
        }

        val numParts = allBytes.head.toInt
        // Check we have enough bytes
        if (allBytes.size < numParts * 2) {
          throw new DeserializationTypeError("truncated len slice")
        }

        // Decode the lengths
        val lengths = Array.ofDim[Int](numParts)
        (0 until numParts).foreach { i =>
          val slice = allBytes.slice(i * 2, i * 2 + 2)
          lengths.update(i, (slice.head & 0xff) << 8 | slice.last & 0xff)
        }

        var buf   = allBytes.drop(numParts * 2)
        val parts = lengths.map { length =>
          val part = buf.take(length)
          buf = buf.drop(length)
          part
        }
        Compound(Chunk.fromArray(parts))
      }

      override def unsafeEncode(a: Compound, output: OutputStream): Unit = ???
    }
  }

}
