package zio.memberlist
import zio.console.putStrLn
import zio.duration._
import zio.memberlist.encoding.MsgPackCodec
import zio.memberlist.protocols.messages.Initial.{NodeViewSnapshot, PushPull}
import zio.memberlist.state.{NodeName, NodeState}
import zio.memberlist.transport.{MemberlistTransport, NetTransport}
import zio.{Chunk, Has, Layer, ZIO, ZLayer}

import java.net.InetAddress

class LocalTransport(port: Int) extends zio.App {

  val dependencies: Layer[TransportError, Has[MemberlistTransport]] =
    ZLayer.succeed(
      MemberlistConfig(
        name = NodeName("local_node_" + port),
        bindAddress = NodeAddress(Chunk.fromArray(InetAddress.getByName("localhost").getAddress), port),
        protocolInterval = 1.second,
        protocolTimeout = 500.milliseconds,
        messageSizeLimit = 64000,
        broadcastResent = 10,
        localHealthMaxMultiplier = 8,
        suspicionAlpha = 9,
        suspicionBeta = 9,
        suspicionRequiredConfirmations = 3
      )
    ) >>> NetTransport.live

//  val program = MemberlistTransport
//    .sendBestEffort(NodeAddress("127.0.0.1", 5559), Chunk.fromArray(("test_" + port).getBytes))
//    .retry(Schedule.spaced(5.seconds))
//    .fork *>
//    MemberlistTransport.receiveBestEffort.collectRight.mapM { case (addr, chunk) =>
//      putStrLn(new String(chunk.toArray))
//    }.runCollect.forever

  val program = {
    for {
      _ <- MemberlistTransport.receiveReliable
             .mapMPar(10) { conn =>
               ZIO.effect {
                 val tag = conn.stream.read()

                 MsgPackCodec[PushPull].unsafeDecode(conn.stream)

                 //s"fixmap: ${InetAddress.getByAddress(addr)}, $incarnation, $meta, $name, $port, $state, $vst"
               }.zipLeft {

                 MsgPackCodec[PushPull]
                   .encode(
                     PushPull(
                       nodes = Chunk.single(
                         NodeViewSnapshot(
                           name = NodeName("local_node_" + port),
                           nodeAddress =
                             NodeAddress(Chunk.fromArray(InetAddress.getByName("localhost").getAddress), port),
                           meta = None,
                           incarnation = 1,
                           state = NodeState.Alive
                         )
                       ),
                       join = true
                     )
                   )
                   .flatMap(payload => MemberlistTransport.sendReliably(conn.id, payload.prepended(6)))
               }
                 .flatMap(msg => putStrLn("" + conn.id + " aaa " + msg))
             }
             .runCollect

    } yield ()
  }

  override def run(args: List[String]) =
    program.provideCustomLayer(dependencies).exitCode
}

object LocalTransport1 extends LocalTransport(5559)
object LocalTransport2 extends LocalTransport(5558)
object LocalTransport3 extends LocalTransport(5557)
