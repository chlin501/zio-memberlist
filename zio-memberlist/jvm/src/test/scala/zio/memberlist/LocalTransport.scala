package zio.memberlist
import upickle.default._
import zio.console.putStrLn
import zio.duration._
import zio.memberlist.state.NodeName
import zio.memberlist.transport.{MemberlistTransport, NetTransport}
import zio.stream.{Transducer, ZStream}
import zio.{Chunk, Has, Layer, ZIO, ZLayer}

import java.io.ByteArrayInputStream

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

  case class PushPullHeader(Nodes: Int, UserStateLen: Int, Join: Boolean)

  case class PushNodeState(
    Name: String,
    Addr: Array[Byte],
    Port: Int,
    Meta: Array[Byte],
    Incarnation: Long,
    State: Int,
    Vsn: Array[Short]
  )

  val program = {
    for {
//      chunk <- ByteCodec[PingPong].toChunk(Ping(2))
//      _     <- MemberlistTransport.sendReliably(NodeAddress("127.0.0.1", 5559), chunk)
      _ <- MemberlistTransport.receiveReliable
             .mapMPar(10) { case (connectionId, stream) =>
               stream
                 .use(input =>
                   ZIO.effect {
                     val tag = input.read()
                     readBinary[PushPullHeader](input)(macroRW[PushPullHeader])
                   }
                 )
                 .flatMap(msg => putStrLn("" + connectionId + " aaa " + msg))
             }
             .runCollect

//      _ <- MemberlistTransport.receiveReliable
//             .flatMapPar(10) { case (connectionId, stream) =>
//               stream
//                 .transduce(Transducer.fromPush {
//                   case Some(chunk) =>
//                     val (tag, msg) = chunk.splitAt(1) //to tylko jak leftover jest puste
//                     val input      = new ByteArrayInputStream(msg.toArray)
//                     val header     = readBinary[PushPullHeader](input)(macroRW[PushPullHeader])
//                     println("sss " + new String(input.readAllBytes()))
//                     //                     val node1      = readBinary[PushNodeState](input)(macroRW[PushNodeState])
//
//                     ZIO.succeedNow(Chunk(header))
//                   case None        => ZIO.succeedNow(Chunk.empty)
//                 })
//                 .mapM(msg => putStrLn("" + connectionId + " aaa " + msg))
//             }
//             .runCollect
    } yield ()
  }

  val s = ZStream.repeatEffectChunkOption {
    ZIO.succeed(Chunk.fromArray("1111".getBytes()))
  }

  val program1 = s
    .transduce(Transducer.fromPush {
      case Some(chunk) => putStrLn(new String(chunk.toArray)).as(Chunk.empty)
      case None        => putStrLn("end").as(Chunk.empty)
    })
    .runCollect

  override def run(args: List[String]) =
    program.provideCustomLayer(dependencies).exitCode
}

object LocalTransport1 extends LocalTransport(5559)
object LocalTransport2 extends LocalTransport(5558)
object LocalTransport3 extends LocalTransport(5557)
