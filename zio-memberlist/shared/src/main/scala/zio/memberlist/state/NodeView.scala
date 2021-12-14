package zio.memberlist.state

import java.time.OffsetDateTime

case class NodeView(node: Node, incarnation: Int, stateChange: OffsetDateTime)
