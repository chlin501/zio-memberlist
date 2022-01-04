package zio.memberlist

import zio.memberlist.encoding.MsgPackCodec

sealed trait TestProtocolMessage

object TestProtocolMessage {

  sealed trait FirstPart extends TestProtocolMessage

  final case class Msg1(i: Int) extends FirstPart

  object Msg1 {

    implicit val msg1Codec: MsgPackCodec[Msg1] =
      MsgPackCodec.int.bimap(i => Msg1(i), _.i)
  }

  final case class Msg2(i: Int) extends FirstPart

  object Msg2 {

    implicit val msg2Codec: MsgPackCodec[Msg2] =
      MsgPackCodec.int.bimap(i => Msg2(i), _.i)
  }

  object FirstPart {
    implicit val codec: MsgPackCodec[FirstPart] =
      MsgPackCodec.tagged[FirstPart][
        Msg1,
        Msg2
      ]
  }

  sealed trait SecondPart extends TestProtocolMessage

  final case class Msg3(i: Int) extends SecondPart

  object Msg3 {

    implicit val msg3Codec: MsgPackCodec[Msg3] =
      MsgPackCodec.int.bimap(i => Msg3(i), _.i)
  }

  implicit val codec: MsgPackCodec[TestProtocolMessage] =
    MsgPackCodec.tagged[TestProtocolMessage][
      Msg1,
      Msg2,
      Msg3
    ]
}
