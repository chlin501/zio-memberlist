//package zio.memberlist.example
//
//import upickle.default.macroRW
//import zio.clock.Clock
//import zio.config.getConfig
//import zio.console.Console
//import zio.duration._
//import zio.logging.{Logging, log}
//import zio.memberlist._
//import zio.memberlist.discovery.Discovery
//import zio.memberlist.encoding.MsgPackCodec
//import zio.nio.core.InetAddress
//import zio.{ExitCode, Has, URIO, ZEnv, ZIO, ZLayer}
//
//object TestNode extends zio.App {
//
//  sealed trait ChaosMonkey
//
//  object ChaosMonkey {
//    final case object SimulateCpuSpike extends ChaosMonkey
//
//    implicit val cpuSpikeCodec: MsgPackCodec[SimulateCpuSpike.type] =
//      MsgPackCodec.fromReadWriter(macroRW[SimulateCpuSpike.type])
//
//    implicit val codec: MsgPackCodec[ChaosMonkey] =
//      MsgPackCodec.tagged[ChaosMonkey][
//        SimulateCpuSpike.type
//      ]
//  }
//
//  import ChaosMonkey._
//
//  val discovery: ZLayer[Has[MemberlistConfig] with Logging, Nothing, Has[Discovery]] =
//    ZLayer.fromManaged(
//      for {
//        appConfig  <- getConfig[MemberlistConfig].toManaged_
//        serviceDns <- InetAddress
//                        .byName("zio.memberlist-node.zio.memberlist-experiment.svc.cluster.local")
//                        .orDie
//                        .toManaged_
//        discovery  <- Discovery.k8Dns(serviceDns, 10.seconds, appConfig.bindAddress.port).build.map(_.get)
//      } yield discovery
//    )
//
//  val dependencies: ZLayer[Console with Clock with zio.system.System, Error, Logging with Has[
//    MemberlistConfig
//  ] with Has[Discovery] with Clock with Has[Memberlist[ChaosMonkey]]] = {
//    val config  = MemberlistConfig.fromEnv.orDie
//    val logging = Logging.console()
//    val seeds   = (logging ++ config) >+> discovery
//    (seeds ++ Clock.live) >+> Memberlist.live[ChaosMonkey]
//  }
//
//  val program: URIO[Has[Memberlist[ChaosMonkey]] with Logging with zio.console.Console, ExitCode] =
//    receive[ChaosMonkey].foreach { case (sender, message) =>
//      log.info(s"receive message: $message from: $sender") *>
//        ZIO.whenCase(message) { case SimulateCpuSpike =>
//          log.info("simulating cpu spike")
//        }
//    }.exitCode
//
//  def run(args: List[String]): URIO[ZEnv with zio.console.Console, ExitCode] =
//    program
//      .provideCustomLayer(dependencies)
//      .exitCode
//
//}
