package zio.memberlist.state

import upickle.default._
import zio.memberlist.encoding.MsgPackCodec

sealed trait NodeState

object NodeState {
  case object Alive   extends NodeState
  case object Suspect extends NodeState
  case object Dead    extends NodeState
  case object Left    extends NodeState

  implicit val nodeStateRW: ReadWriter[NodeState] =
    macroRW[NodeState]

  implicit val nodeStateCodec: MsgPackCodec[NodeState] = MsgPackCodec.int.bimap(
    {
      case 0 => Alive
      case 1 => Suspect
      case 2 => Dead
      case 3 => Left
    },
    {
      case Alive   => 0
      case Suspect => 1
      case Dead    => 2
      case Left    => 3
    }
  )
}
