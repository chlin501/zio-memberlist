//package zio.memberlist
//
//import upickle.default._
//import zio.memberlist.encoding.MsgPackCodec
//
//sealed trait PingPong
//
//object PingPong {
//  final case class Ping(i: Int) extends PingPong
//
//  object Ping {
//
//    implicit val pingCodec: MsgPackCodec[Ping] =
//      MsgPackCodec.fromReadWriter(macroRW[Ping])
//  }
//
//  final case class Pong(i: Int) extends PingPong
//
//  object Pong {
//
//    implicit val pongCodec: MsgPackCodec[Pong] =
//      MsgPackCodec.fromReadWriter(macroRW[Pong])
//  }
//
//  implicit val codec: MsgPackCodec[PingPong] =
//    MsgPackCodec.tagged[PingPong][
//      Ping,
//      Pong
//    ]
//}
