package zio.memberlist
import zio.console.putStrLn
import zio.duration._
import zio.memberlist.state.NodeName
import zio.memberlist.transport.{MemberlistTransport, NetTransport}
import zio.{Chunk, Has, Layer, ZLayer}

class LocalTransport(port: Int) extends zio.App {

  val dependencies: Layer[TransportError, Has[MemberlistTransport]] =
    ZLayer.succeed(
      MemberlistConfig(
        name = NodeName("local_node_" + port),
        bindAddress = NodeAddress("localhost", port),
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
    MemberlistTransport
      .sendReliably(NodeAddress("127.0.0.1", 5559), Chunk.fromArray(("test_" + port).getBytes))
//      .retry(Schedule.spaced(5.seconds))
      .fork *>
      MemberlistTransport.receiveReliable.mapM { case (connectionId, chunk) =>
        putStrLn("receive connection " + connectionId) *>
          chunk
            .grouped(4)
            .mapM(chunk => putStrLn(new String(chunk.toArray)))
            .runDrain
            .fork

      }.runCollect

  }
//  val program = ZStream
//    .repeatEffectWith(putStrLn("left"), Schedule.spaced(5.seconds))
//    .merge(ZStream.repeatEffectWith(putStrLn("right"), Schedule.spaced(1.seconds)))
//    .runDrain
//    .exitCode

  override def run(args: List[String]) =
    program.provideCustomLayer(dependencies).exitCode
}

object LocalTransport1 extends LocalTransport(5559)
object LocalTransport2 extends LocalTransport(5558)
object LocalTransport3 extends LocalTransport(5557)
