package zio.memberlist.state

import upickle.default._

sealed trait NodeState

object NodeState {
  case object Alive   extends NodeState
  case object Suspect extends NodeState
  case object Dead    extends NodeState
  case object Left    extends NodeState

  implicit val nodeStateRW: ReadWriter[NodeState] =
    macroRW[NodeState]
}
