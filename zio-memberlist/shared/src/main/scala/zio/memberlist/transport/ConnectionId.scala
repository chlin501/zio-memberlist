package zio.memberlist.transport

import zio.UIO
import zio.memberlist.uuid

import java.util.UUID

case class ConnectionId(uuid: UUID) extends AnyVal

object ConnectionId {
  val make: UIO[ConnectionId] = uuid.makeRandomUUID.map(ConnectionId(_))
}
