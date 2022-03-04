package zio.memberlist.protocols

import zio.clock.Clock
import zio.duration._
import zio.logging.Logger
import zio.memberlist.protocols.messages.FailureDetection._
import zio.memberlist.state.{NodeName, NodeState, Nodes}
import zio.memberlist.transport.ConnectionId
import zio.memberlist.{MessageSequenceNo, _}
import zio.stm.{STM, TMap, ZSTM}
import zio.stream.ZStream
import zio.{Chunk, Has, IO, Schedule, ZIO, stream}

class FailureDetection(
  protocolPeriod: Duration,
  protocolTimeout: Duration,
  localNode: NodeName,
  localAddress: NodeAddress,
  messageAcknowledge: MessageAcknowledge,
  log: Logger[String],
  nodes: Nodes,
  incarnationSequence: IncarnationSequence,
  messageSequenceNo: MessageSequenceNo,
  pingReqs: TMap[Long, (NodeAddress, Long)],
  suspicionTimeout: SuspicionTimeout,
  localHealthMultiplier: LocalHealthMultiplier,
  clock: Clock.Service
) extends Protocol[messages.FailureDetection] {

  override def onBestEffortMessage
    : (NodeAddress, messages.FailureDetection) => IO[Error, Message[messages.FailureDetection]] = {
    case (sender, msg @ Ack(seqNo, _)) =>
      log.debug(s"received ack[$seqNo] from $sender") *>
        ZSTM.atomically(
          messageAcknowledge.ack(msg) *>
            pingReqs.get(seqNo).map {
              case Some((originalNode, originalSeqNo)) =>
                Message.BestEffortByAddress(originalNode, Ack(originalSeqNo, Chunk.empty))
              case None                                =>
                Message.NoResponse
            } <* localHealthMultiplier.decrease
        )

    case (_, Ping(seqNo, _, sourceAddr, _)) =>
      ZIO.succeedNow(Message.BestEffortByAddress(sourceAddr, Ack(seqNo, Chunk.empty)))

    case (sender, PingReq(originalSeqNo, to)) =>
      messageSequenceNo.next
        .map(newSeqNo => Ping(newSeqNo, to, localAddress, localNode))
        .flatMap(ping =>
          pingReqs.put(ping.seqNo, (sender, originalSeqNo)) *>
            Message.withTimeout(
              message = Message.BestEffortByName(to, ping),
              action = pingReqs
                .get(ping.seqNo)
                .flatMap {
                  case Some((sender, originalSeqNo)) =>
                    pingReqs
                      .delete(ping.seqNo)
                      .as(Message.BestEffortByAddress(sender, Nack(originalSeqNo)))
                  case _                             =>
                    STM.succeedNow(Message.NoResponse)
                }
                .commit,
              timeout = protocolTimeout
            )
        )
        .commit
    case (_, msg @ Nack(_))                   =>
      messageAcknowledge.ack(msg).commit *>
        Message.noResponse

    case (sender, Suspect(accusedIncarnation, _, `localNode`)) =>
      ZSTM.atomically(
        incarnationSequence.nextAfter(accusedIncarnation).map { incarnation =>
          val alive = Alive(incarnation, localNode)
          Message.Batch(Message.BestEffortByAddress(sender, alive), Message.Broadcast(alive))
        } <* localHealthMultiplier.increase
      )

    case (from, Suspect(incarnation, _, node)) =>
      incarnationSequence.current
        .zip(
          nodes.nodeState(node).orElseSucceed(NodeState.Dead)
        )
        .flatMap {
          case (localIncarnation, _) if localIncarnation > incarnation =>
            STM.unit
          case (_, NodeState.Dead)                                     =>
            STM.unit
          case (_, NodeState.Suspect)                                  =>
            STM.unit
          //SuspicionTimeout.incomingSuspect(node, from)
          case (_, _)                                                  =>
            nodes.changeNodeState(node, NodeState.Suspect).ignore
        }
        .commit *> Message.noResponse

    case (sender, msg @ Dead(_, _, nodeAddress)) if sender == nodeAddress =>
      nodes
        .changeNodeState(nodeAddress, NodeState.Left)
        .ignore
        .as(Message.Broadcast(msg))
        .commit

    case (_, msg @ Dead(_, _, nodeAddress)) =>
      nodes
        .nodeState(nodeAddress)
        .orElseSucceed(NodeState.Dead)
        .flatMap {
          case NodeState.Dead =>
            STM.succeed(Message.NoResponse)
          case _              =>
            nodes
              .changeNodeState(nodeAddress, NodeState.Dead)
              .ignore
              .as(Message.Broadcast(msg))
        }
        .commit

    case (_, msg @ Alive(_, nodeAddress)) =>
      ZSTM.atomically(
        suspicionTimeout.cancelTimeout(nodeAddress) *>
          nodes
            .changeNodeState(nodeAddress, NodeState.Alive)
            .ignore
            .as(Message.Broadcast(msg))
      )
  }

  override def onReliableMessage
    : (ConnectionId, messages.FailureDetection) => IO[Error, Message[messages.FailureDetection]] = (_, _) =>
    Message.noResponse

  override val produceMessages: stream.Stream[Error, Message[messages.FailureDetection]] =
    ZStream
      .repeatEffectWith(
        nodes.next(exclude = Option(localNode)).zip(messageSequenceNo.next).commit,
        Schedule.forever.addDelayM(_ => localHealthMultiplier.scaleTimeout(protocolPeriod).commit)
      )
      .provide(Has(clock))
      .collectM { case (Some((probedNodeName, probeNode)), seqNo) =>
        ZSTM.atomically(
          messageAcknowledge.register(Ack(seqNo, Chunk.empty)) *>
            incarnationSequence.current.flatMap(incarnation =>
              localHealthMultiplier
                .scaleTimeout(protocolTimeout)
                .map(timeout =>
                  Message.WithTimeout(
                    if (probeNode.state != NodeState.Alive)
                      Message.Batch(
                        Message.BestEffortByName(probedNodeName, Ping(seqNo, probedNodeName, localAddress, localNode)),
                        //this is part of buddy system
                        Message.BestEffortByName(probedNodeName, Suspect(incarnation, localNode, probedNodeName))
                      )
                    else
                      Message.BestEffortByName(probedNodeName, Ping(seqNo, probedNodeName, localAddress, localNode)),
                    pingTimeoutAction(
                      seqNo,
                      probedNodeName,
                      localNode,
                      protocolTimeout
                    ),
                    timeout
                  )
                )
            )
        )
      }

  private def pingTimeoutAction(
    seqNo: Long,
    probedNode: NodeName,
    localNode: NodeName,
    protocolTimeout: Duration
  ): IO[Error, Message[messages.FailureDetection]] =
    ZIO.ifM(messageAcknowledge.isCompleted(Ack(seqNo, Chunk.empty)).commit)(
      Message.noResponse,
      log.warn(s"node: $probedNode missed ack with id ${seqNo}") *>
        ZSTM.atomically(
          localHealthMultiplier.increase *>
            nodes.next(Some(probedNode)).flatMap {
              case Some((next, _)) =>
                messageAcknowledge.register(Nack(seqNo)) *>
                  localHealthMultiplier
                    .scaleTimeout(protocolTimeout)
                    .map(timeout =>
                      Message.WithTimeout(
                        Message.BestEffortByName(next, PingReq(seqNo, probedNode)),
                        pingReqTimeoutAction(
                          seqNo = seqNo,
                          probedNode = probedNode,
                          localNode = localNode
                        ),
                        timeout
                      )
                    )
              case None            =>
                // we don't know any other node to ask
                nodes.changeNodeState(probedNode, NodeState.Dead).as(Message.NoResponse)
            }
        )
    )

  private def pingReqTimeoutAction(
    seqNo: Long,
    probedNode: NodeName,
    localNode: NodeName
  ): IO[Error, Message[messages.FailureDetection]] =
    ZIO.ifM(messageAcknowledge.isCompleted(Ack(seqNo, Chunk.empty)).commit)(
      Message.noResponse,
      ZSTM.atomically(
        messageAcknowledge.ack(Ack(seqNo, Chunk.empty)) *>
          (localHealthMultiplier.increase *> messageAcknowledge.ack(Nack(seqNo)))
            .whenM(messageAcknowledge.isCompleted(Nack(seqNo)).map(!_)) *>
          nodes.changeNodeState(probedNode, NodeState.Suspect) *>
          incarnationSequence.current.flatMap(currentIncarnation =>
            Message.withTimeout(
              Message.Broadcast(Suspect(currentIncarnation, localNode, probedNode)),
              suspicionTimeout
                .registerTimeout(probedNode)
                .commit
                .flatMap(_.awaitAction)
                .orElse(Message.noResponse),
              Duration.Zero
            )
          )
      )
    )
  //protocol.transformM(compress)

}
