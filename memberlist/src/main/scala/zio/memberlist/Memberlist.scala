package zio.memberlist

import izumi.reflect.Tag
import zio.clock.Clock
import zio.config._
import zio.logging._
import zio.memberlist.discovery.Discovery
import zio.memberlist.encoding.ByteCodec
import zio.memberlist.state._
import zio.memberlist.protocols.{ FailureDetection, User }
import zio.stream.{ Stream, ZStream }
import zio.{ Chunk, IO, Queue, UIO, ZLayer, ZManaged }
import zio.memberlist.protocols.messages
import zio.memberlist.protocols.messages.MemberlistMessage

object Memberlist {

  trait Service[A] {
    def broadcast(data: A): IO[zio.memberlist.Error, Unit]
    def events: Stream[Nothing, MembershipEvent]
    def localMember: NodeName
    def nodes: UIO[Set[NodeName]]
    def receive: Stream[Nothing, (NodeName, A)]
    def send(data: A, receipt: NodeName): UIO[Unit]
  }

  type Env = ZConfig[MemberlistConfig] with Discovery with Logging with Clock

  final private[this] val QueueSize = 1000

  def live[B: ByteCodec: Tag]: ZLayer[Env, Error, Memberlist[B]] = {
    val internalLayer =
      (ZLayer.requires[Env] ++
        MessageSequenceNo.live ++
        IncarnationSequence.live ++
        LocalHealthMultiplier.liveWithConfig ++
        Nodes.liveWithConfig ++
        MessageAcknowledge.live) >+> SuspicionTimeout.liveWithConfig

    val managed =
      for {
        env           <- ZManaged.environment[MessageSequence with Nodes]
        localConfig   <- config[MemberlistConfig].toManaged_
        _             <- log.info("starting SWIM on port: " + localConfig.port).toManaged_
        udpTransport  <- transport.udp.live(localConfig.messageSizeLimit).build.map(_.get)
        userIn        <- Queue.bounded[Message.BestEffort[B]](QueueSize).toManaged(_.shutdown)
        userOut       <- Queue.bounded[Message.BestEffort[B]](QueueSize).toManaged(_.shutdown)
        localNodeName = localConfig.name
        //        initial          <- Initial.protocol(localNodeAddress).toManaged_
        failureDetection <- FailureDetection
                             .protocol(localConfig.protocolInterval, localConfig.protocolTimeout, localNodeName)
                             .toManaged_

        user <- User.protocol[B](userIn, userOut).toManaged_
        //        deadLetter   <- DeadLetter.protocol.toManaged_
        allProtocols = Protocol.compose[MemberlistMessage](failureDetection, user).binary
        broadcast0   <- Broadcast.make(localConfig.messageSizeLimit, localConfig.broadcastResent).toManaged_
        localAddress <- NodeAddress.local(localConfig.port).toManaged_
        messages0    <- MessageSink.make(localNodeName, localAddress, broadcast0, udpTransport)
        _            <- messages0.process(allProtocols).toManaged_
        localNode    = Node(localConfig.name, localAddress, Chunk.empty, NodeState.Alive)
        _            <- env.get[Nodes.Service].addNode(localNode).commit.toManaged_
      } yield new Memberlist.Service[B] {

        override def broadcast(data: B): IO[SerializationError, Unit] =
          for {
            bytes <- ByteCodec.encode[messages.User[B]](messages.User(data))
            _     <- broadcast0.add(Message.Broadcast(bytes))
          } yield ()

        override val receive: Stream[Nothing, (NodeName, B)] =
          ZStream.fromQueue(userIn).collect {
            case Message.BestEffort(n, m) => (n, m)
          }

        override def send(data: B, receipt: NodeName): UIO[Unit] =
          userOut.offer(Message.BestEffort(receipt, data)).unit

        override def events: Stream[Nothing, MembershipEvent] =
          env.get[Nodes.Service].events

        override def localMember: NodeName = localNodeName

        override def nodes: UIO[Set[NodeName]] =
          env.get[Nodes.Service].healthyNodes.map(_.map(_._1).toSet).commit
      }

    internalLayer >>> ZLayer.fromManaged(managed)
  }
}
