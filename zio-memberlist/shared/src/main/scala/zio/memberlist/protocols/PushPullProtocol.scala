package zio.memberlist.protocols

import zio.memberlist.discovery._
import zio.memberlist.protocols.messages.Initial._
import zio.memberlist.state.Nodes
import zio.memberlist.transport.ConnectionId
import zio.memberlist.{NodeAddress, Protocol, _}
import zio.stm.ZSTM
import zio.{Chunk, IO, ZIO, stream}

class PushPullProtocol(discovery: Discovery, nodes: Nodes, incarnation: IncarnationSequence)
    extends Protocol[PushPull] {

  override def onBestEffortMessage: (NodeAddress, PushPull) => IO[Error, Message[PushPull]] =
    (_, _) => Message.noResponse

  override def onReliableMessage: (ConnectionId, PushPull) => IO[Error, Message[PushPull]] = { case (id, msg) =>
    ZSTM.atomically {
      for {
        allNodes           <- nodes.allNodes
        currentIncarnation <- incarnation.current
      } yield Message.ReliableByConnection(
        connectionId = id,
        message = PushPull(
          Chunk.fromIterable(allNodes).map(n => NodeViewSnapshot(n.name, n.addr, n.meta, currentIncarnation, n.state)),
          msg.join
        )
      )
    }

  }

  override val produceMessages: stream.Stream[Error, Message[PushPull]] =
    zio.stream.Stream.fromIterableM(
      for {
        remoteNodes        <- discovery.discoverNodes
        addresses          <- ZIO.foreach(remoteNodes)(NodeAddress.fromSocketAddress)
        localState         <- nodes.allNodes.commit
        currentIncarnation <- incarnation.current.commit
      } yield {
        val msg = PushPull(
          nodes = Chunk
            .fromIterable(localState)
            .map(n => NodeViewSnapshot(n.name, n.addr, n.meta, currentIncarnation, n.state)),
          join = true
        )
        addresses.map(addr => Message.ReliableByAddress(addr, msg))
      }
    )
}
