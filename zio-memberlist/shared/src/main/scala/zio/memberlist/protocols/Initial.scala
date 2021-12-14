package zio.memberlist.protocols

import zio.logging._
import zio.memberlist.discovery._
import zio.memberlist.protocols.messages.Initial._
import zio.memberlist.state.{Node, NodeName, NodeState, Nodes}
import zio.memberlist.{NodeAddress, Protocol, _}
import zio.stm.ZSTM
import zio.stream.ZStream
import zio.{Chunk, Has, ZIO}

object Initial {

  type Env = Has[MessageSequenceNo] with Has[Nodes] with Logging with Has[Discovery]

  def protocol(localAddr: NodeAddress, localName: NodeName): ZIO[Env, Error, Protocol[messages.Initial]] =
    Protocol[messages.Initial].make(
      {
        //to powinna byc tupla
        case (addr, PushPull(state, join)) =>
          //musimy trzymac jakos connection zeby odpowiedziec tym samym
          ???
      },
      ZStream
        .fromIterableM(Discovery.discoverNodes.tap(otherNodes => log.info("Discovered other nodes: " + otherNodes)))
        .mapM { inetSocketAddress =>
          NodeAddress
            .fromSocketAddress(inetSocketAddress)
            .map(nodeAddress => Message.ReliableByAddress(nodeAddress, PushPull(Chunk.empty, true)))
        }
    )

}
