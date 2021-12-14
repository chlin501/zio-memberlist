package zio.memberlist.protocols

import zio.ZIO
import zio.memberlist.encoding.ByteCodec
import zio.memberlist.{Protocol, _}
import zio.stream._

object User {

  def protocol[B: ByteCodec](
    userIn: zio.Queue[Message[B]],
    userOut: zio.Queue[Message[B]]
  ): ZIO[Any, Error, Protocol[messages.User[B]]] =
    Protocol[messages.User[B]].make(
      (addr, msg) => userIn.offer(Message.BestEffortByAddress(addr, msg.msg)).as(Message.NoResponse),
      ZStream
        .fromQueue(userOut)
        .collect { case Message.BestEffortByName(node, msg) =>
          Message.BestEffortByName(node, messages.User(msg))
        }
    )

}
