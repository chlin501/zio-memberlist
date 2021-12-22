package zio.memberlist.encoding

import upack.MsgPackKeys
import zio.memberlist.SerializationError.{DeserializationTypeError, SerializationTypeError}

import java.io.{InputStream, OutputStream}
import scala.annotation.switch
import scala.reflect.ClassTag

trait MsgPackCodec[A] { self =>
  def unsafeDecode(input: InputStream): A
  def unsafeEncode(a: A, output: OutputStream): Unit

  def zip[B](that: MsgPackCodec[B]): MsgPackCodec[(A, B)] = new MsgPackCodec[(A, B)] {
    override def unsafeDecode(input: InputStream): (A, B) =
      (self.unsafeDecode(input), that.unsafeDecode(input))

    override def unsafeEncode(a: (A, B), output: OutputStream): Unit = {
      self.unsafeEncode(a._1, output)
      that.unsafeEncode(a._2, output)
    }
  }

  def bimap[B](f: A => B, g: B => A): MsgPackCodec[B] = new MsgPackCodec[B] {
    override def unsafeDecode(input: InputStream): B = f(self.unsafeDecode(input))

    override def unsafeEncode(a: B, output: OutputStream): Unit = self.unsafeEncode(g(a), output)
  }

  private[MsgPackCodec] def unsafeWiden[A1](implicit ev: A1 <:< A): MsgPackCodec[A1] =
    new MsgPackCodec[A1] {

      override def unsafeDecode(input: InputStream): A1 = self.unsafeDecode(input).asInstanceOf[A1]

      override def unsafeEncode(a1: A1, output: OutputStream): Unit =
        self.unsafeEncode(ev(a1), output)
    }
}

object MsgPackCodec {

  final class TaggedBuilder[A] {

    def apply[A1 <: A: MsgPackCodec: ClassTag](implicit ev: A <:< A1): MsgPackCodec[A] =
      taggedInstance[A](
        { case _: A1 =>
          0
        },
        { case 0 =>
          MsgPackCodec[A1].unsafeWiden[A]
        }
      )

    def apply[A1 <: A: MsgPackCodec: ClassTag, A2 <: A: MsgPackCodec: ClassTag](implicit
      ev1: A <:< A1,
      ev2: A <:< A2
    ): MsgPackCodec[A] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
        },
        {
          case 0 => MsgPackCodec[A1].unsafeWiden[A]
          case 1 => MsgPackCodec[A2].unsafeWiden[A]
        }
      )

    def apply[
      A1 <: A: MsgPackCodec: ClassTag,
      A2 <: A: MsgPackCodec: ClassTag,
      A3 <: A: MsgPackCodec: ClassTag
    ](implicit ev1: A <:< A1, ev2: A <:< A2, ev3: A <:< A3): MsgPackCodec[A] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
          case _: A3 => 2
        },
        {
          case 0 => MsgPackCodec[A1].unsafeWiden[A]
          case 1 => MsgPackCodec[A2].unsafeWiden[A]
          case 2 => MsgPackCodec[A3].unsafeWiden[A]
        }
      )

    def apply[
      A1 <: A: MsgPackCodec: ClassTag,
      A2 <: A: MsgPackCodec: ClassTag,
      A3 <: A: MsgPackCodec: ClassTag,
      A4 <: A: MsgPackCodec: ClassTag
    ](implicit ev1: A <:< A1, ev2: A <:< A2, ev3: A <:< A3, ev4: A <:< A4): MsgPackCodec[A] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
          case _: A3 => 2
          case _: A4 => 3
        },
        {
          case 0 => MsgPackCodec[A1].unsafeWiden[A]
          case 1 => MsgPackCodec[A2].unsafeWiden[A]
          case 2 => MsgPackCodec[A3].unsafeWiden[A]
          case 3 => MsgPackCodec[A4].unsafeWiden[A]
        }
      )

    def apply[
      A1 <: A: MsgPackCodec: ClassTag,
      A2 <: A: MsgPackCodec: ClassTag,
      A3 <: A: MsgPackCodec: ClassTag,
      A4 <: A: MsgPackCodec: ClassTag,
      A5 <: A: MsgPackCodec: ClassTag
    ](implicit ev1: A <:< A1, ev2: A <:< A2, ev3: A <:< A3, ev4: A <:< A4, ev5: A <:< A5): MsgPackCodec[A] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
          case _: A3 => 2
          case _: A4 => 3
          case _: A5 => 4
        },
        {
          case 0 => MsgPackCodec[A1].unsafeWiden[A]
          case 1 => MsgPackCodec[A2].unsafeWiden[A]
          case 2 => MsgPackCodec[A3].unsafeWiden[A]
          case 3 => MsgPackCodec[A4].unsafeWiden[A]
          case 4 => MsgPackCodec[A5].unsafeWiden[A]
        }
      )

    def apply[
      A1 <: A: MsgPackCodec: ClassTag,
      A2 <: A: MsgPackCodec: ClassTag,
      A3 <: A: MsgPackCodec: ClassTag,
      A4 <: A: MsgPackCodec: ClassTag,
      A5 <: A: MsgPackCodec: ClassTag,
      A6 <: A: MsgPackCodec: ClassTag
    ](implicit
      ev1: A <:< A1,
      ev2: A <:< A2,
      ev3: A <:< A3,
      ev4: A <:< A4,
      ev5: A <:< A5,
      ev6: A <:< A6
    ): MsgPackCodec[A] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
          case _: A3 => 2
          case _: A4 => 3
          case _: A5 => 4
          case _: A6 => 5
        },
        {
          case 0 => MsgPackCodec[A1].unsafeWiden[A]
          case 1 => MsgPackCodec[A2].unsafeWiden[A]
          case 2 => MsgPackCodec[A3].unsafeWiden[A]
          case 3 => MsgPackCodec[A4].unsafeWiden[A]
          case 4 => MsgPackCodec[A5].unsafeWiden[A]
          case 5 => MsgPackCodec[A6].unsafeWiden[A]
        }
      )

    def apply[
      A1 <: A: MsgPackCodec: ClassTag,
      A2 <: A: MsgPackCodec: ClassTag,
      A3 <: A: MsgPackCodec: ClassTag,
      A4 <: A: MsgPackCodec: ClassTag,
      A5 <: A: MsgPackCodec: ClassTag,
      A6 <: A: MsgPackCodec: ClassTag,
      A7 <: A: MsgPackCodec: ClassTag
    ](implicit
      ev1: A <:< A1,
      ev2: A <:< A2,
      ev3: A <:< A3,
      ev4: A <:< A4,
      ev5: A <:< A5,
      ev6: A <:< A6,
      ev7: A <:< A7
    ): MsgPackCodec[A] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
          case _: A3 => 2
          case _: A4 => 3
          case _: A5 => 4
          case _: A6 => 5
          case _: A7 => 6
        },
        {
          case 0 => MsgPackCodec[A1].unsafeWiden[A]
          case 1 => MsgPackCodec[A2].unsafeWiden[A]
          case 2 => MsgPackCodec[A3].unsafeWiden[A]
          case 3 => MsgPackCodec[A4].unsafeWiden[A]
          case 4 => MsgPackCodec[A5].unsafeWiden[A]
          case 5 => MsgPackCodec[A6].unsafeWiden[A]
          case 6 => MsgPackCodec[A7].unsafeWiden[A]
        }
      )

    def apply[
      A1 <: A: MsgPackCodec: ClassTag,
      A2 <: A: MsgPackCodec: ClassTag,
      A3 <: A: MsgPackCodec: ClassTag,
      A4 <: A: MsgPackCodec: ClassTag,
      A5 <: A: MsgPackCodec: ClassTag,
      A6 <: A: MsgPackCodec: ClassTag,
      A7 <: A: MsgPackCodec: ClassTag,
      A8 <: A: MsgPackCodec: ClassTag
    ](implicit
      ev1: A <:< A1,
      ev2: A <:< A2,
      ev3: A <:< A3,
      ev4: A <:< A4,
      ev5: A <:< A5,
      ev6: A <:< A6,
      ev7: A <:< A7,
      ev8: A <:< A8
    ): MsgPackCodec[A] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
          case _: A3 => 2
          case _: A4 => 3
          case _: A5 => 4
          case _: A6 => 5
          case _: A7 => 6
          case _: A8 => 7
        },
        {
          case 0 => MsgPackCodec[A1].unsafeWiden[A]
          case 1 => MsgPackCodec[A2].unsafeWiden[A]
          case 2 => MsgPackCodec[A3].unsafeWiden[A]
          case 3 => MsgPackCodec[A4].unsafeWiden[A]
          case 4 => MsgPackCodec[A5].unsafeWiden[A]
          case 5 => MsgPackCodec[A6].unsafeWiden[A]
          case 6 => MsgPackCodec[A7].unsafeWiden[A]
          case 7 => MsgPackCodec[A8].unsafeWiden[A]
        }
      )

    def apply[
      A1 <: A: MsgPackCodec: ClassTag,
      A2 <: A: MsgPackCodec: ClassTag,
      A3 <: A: MsgPackCodec: ClassTag,
      A4 <: A: MsgPackCodec: ClassTag,
      A5 <: A: MsgPackCodec: ClassTag,
      A6 <: A: MsgPackCodec: ClassTag,
      A7 <: A: MsgPackCodec: ClassTag,
      A8 <: A: MsgPackCodec: ClassTag,
      A9 <: A: MsgPackCodec: ClassTag
    ](implicit
      ev1: A <:< A1,
      ev2: A <:< A2,
      ev3: A <:< A3,
      ev4: A <:< A4,
      ev5: A <:< A5,
      ev6: A <:< A6,
      ev7: A <:< A7,
      ev8: A <:< A8,
      ev9: A <:< A9
    ): MsgPackCodec[A] =
      taggedInstance[A](
        {
          case _: A1 => 0
          case _: A2 => 1
          case _: A3 => 2
          case _: A4 => 3
          case _: A5 => 4
          case _: A6 => 5
          case _: A7 => 6
          case _: A8 => 7
          case _: A9 => 8
        },
        {
          case 0 => MsgPackCodec[A1].unsafeWiden[A]
          case 1 => MsgPackCodec[A2].unsafeWiden[A]
          case 2 => MsgPackCodec[A3].unsafeWiden[A]
          case 3 => MsgPackCodec[A4].unsafeWiden[A]
          case 4 => MsgPackCodec[A5].unsafeWiden[A]
          case 5 => MsgPackCodec[A6].unsafeWiden[A]
          case 6 => MsgPackCodec[A7].unsafeWiden[A]
          case 7 => MsgPackCodec[A8].unsafeWiden[A]
          case 8 => MsgPackCodec[A9].unsafeWiden[A]
        }
      )

    def apply[
      A1 <: A: MsgPackCodec: ClassTag,
      A2 <: A: MsgPackCodec: ClassTag,
      A3 <: A: MsgPackCodec: ClassTag,
      A4 <: A: MsgPackCodec: ClassTag,
      A5 <: A: MsgPackCodec: ClassTag,
      A6 <: A: MsgPackCodec: ClassTag,
      A7 <: A: MsgPackCodec: ClassTag,
      A8 <: A: MsgPackCodec: ClassTag,
      A9 <: A: MsgPackCodec: ClassTag,
      A10 <: A: MsgPackCodec: ClassTag
    ](implicit
      ev1: A <:< A1,
      ev2: A <:< A2,
      ev3: A <:< A3,
      ev4: A <:< A4,
      ev5: A <:< A5,
      ev6: A <:< A6,
      ev7: A <:< A7,
      ev8: A <:< A8,
      ev9: A <:< A9,
      ev10: A <:< A10
    ): MsgPackCodec[A] =
      taggedInstance[A](
        {
          case _: A1  => 0
          case _: A2  => 1
          case _: A3  => 2
          case _: A4  => 3
          case _: A5  => 4
          case _: A6  => 5
          case _: A7  => 6
          case _: A8  => 7
          case _: A9  => 8
          case _: A10 => 9
        },
        {
          case 0 => MsgPackCodec[A1].unsafeWiden[A]
          case 1 => MsgPackCodec[A2].unsafeWiden[A]
          case 2 => MsgPackCodec[A3].unsafeWiden[A]
          case 3 => MsgPackCodec[A4].unsafeWiden[A]
          case 4 => MsgPackCodec[A5].unsafeWiden[A]
          case 5 => MsgPackCodec[A6].unsafeWiden[A]
          case 6 => MsgPackCodec[A7].unsafeWiden[A]
          case 7 => MsgPackCodec[A8].unsafeWiden[A]
          case 8 => MsgPackCodec[A9].unsafeWiden[A]
          case 9 => MsgPackCodec[A10].unsafeWiden[A]
        }
      )

  }

  def apply[A](implicit ev: MsgPackCodec[A]): MsgPackCodec[A] =
    ev

  def tagged[A]: TaggedBuilder[A] =
    new TaggedBuilder[A]()

  def taggedInstance[A](f: PartialFunction[A, Byte], g: PartialFunction[Byte, MsgPackCodec[A]]): MsgPackCodec[A] = {

    def tagOf(a: A): Byte =
      if (f.isDefinedAt(a)) f(a)
      else -1

    def codecFor(tag: Byte): MsgPackCodec[A] =
      if (g.isDefinedAt(tag)) g(tag)
      else null

    new MsgPackCodec[A] {

      override def unsafeDecode(input: InputStream): A = {
        val tag   = input.read()
        val codec = codecFor(tag.toByte)
        if (codec != null) {
          codec.unsafeDecode(input)
        } else {
          throw DeserializationTypeError("Unknown tag: " + tag)
        }
      }

      override def unsafeEncode(a: A, output: OutputStream): Unit = {
        val tag   = tagOf(a)
        if (tag == -1) throw SerializationTypeError("Cannot find tag for: " + a.getClass.getName)
        val codec = codecFor(tag)
        if (codec == null) throw SerializationTypeError("Cannot find codec for: " + tag)
        codec.unsafeEncode(a, output)
      }
    }
  }

  private def writeUInt8(i: Int, outputStream: OutputStream): Unit =
    outputStream.write(i.toByte)

  private def writeUInt16(i: Int, outputStream: OutputStream): Unit = {
    outputStream.write(((i >> 8) & 0xff).toByte)
    outputStream.write(((i >> 0) & 0xff).toByte)
  }
  private def writeUInt32(i: Int, outputStream: OutputStream): Unit = {
    outputStream.write(((i >> 24) & 0xff).toByte)
    outputStream.write(((i >> 16) & 0xff).toByte)
    outputStream.write(((i >> 8) & 0xff).toByte)
    outputStream.write(((i >> 0) & 0xff).toByte)
  }
  private def writeUInt64(i: Long, outputStream: OutputStream): Unit = {
    outputStream.write(((i >> 56) & 0xff).toByte)
    outputStream.write(((i >> 48) & 0xff).toByte)
    outputStream.write(((i >> 40) & 0xff).toByte)
    outputStream.write(((i >> 32) & 0xff).toByte)
    outputStream.write(((i >> 24) & 0xff).toByte)
    outputStream.write(((i >> 16) & 0xff).toByte)
    outputStream.write(((i >> 8) & 0xff).toByte)
    outputStream.write(((i >> 0) & 0xff).toByte)
  }

  def parseUInt8(inputStream: InputStream): Int =
    inputStream.read() & 0xff

  def parseUInt16(inputStream: InputStream): Int =
    (inputStream.read() & 0xff) << 8 | inputStream.read() & 0xff

  def parseUInt32(inputStream: InputStream): Int =
    (inputStream.read() & 0xff) << 24 | (inputStream.read() & 0xff) << 16 |
      (inputStream.read() & 0xff) << 8 | inputStream.read() & 0xff

  def parseUInt64(inputStream: InputStream): Long =
    (inputStream.read().toLong & 0xff) << 56 | (inputStream.read().toLong & 0xff) << 48 |
      (inputStream.read().toLong & 0xff) << 40 | (inputStream.read().toLong & 0xff) << 32 |
      (inputStream.read().toLong & 0xff) << 24 | (inputStream.read().toLong & 0xff) << 16 |
      (inputStream.read().toLong & 0xff) << 8 | (inputStream.read().toLong & 0xff) << 0

  //FIXME I don't think that this is correct way of handle that but this is how go implementation is doing this
  implicit val byteArray = new MsgPackCodec[Array[Byte]] {
    override def unsafeDecode(input: InputStream): Array[Byte] = {
      val n = input.read()
      (n & 0xff: @switch) match {
        case MsgPackKeys.Nil   => null
        case MsgPackKeys.Str8  =>
          input.readNBytes(parseUInt8(input))
        case MsgPackKeys.Str16 => input.readNBytes(parseUInt16(input))
        case MsgPackKeys.Str32 => input.readNBytes(parseUInt32(input))
        case x                 => input.readNBytes(x & 0x1f)
      }
    }

    override def unsafeEncode(s: Array[Byte], output: OutputStream): Unit = {
      val length = s.length
      if (length <= 31) {
        output.write((MsgPackKeys.FixStrMask | length).toByte)
      } else if (length <= 255) {
        output.write(MsgPackKeys.Str8.toByte)
        writeUInt8(length, output)
      } else if (length <= 65535) {
        output.write(MsgPackKeys.Str16.toByte)
        writeUInt16(length, output)
      } else {
        output.write(MsgPackKeys.Str32.toByte)
        writeUInt32(length, output)
      }
      output.write(s)
    }
  }

  implicit val string = new MsgPackCodec[String] {
    override def unsafeDecode(input: InputStream): String = {
      val n = input.read()
      (n & 0xff: @switch) match {
        case MsgPackKeys.Nil   => null
        case MsgPackKeys.Str8  =>
          new String(input.readNBytes(parseUInt8(input)), java.nio.charset.StandardCharsets.UTF_8)
        case MsgPackKeys.Str16 => new String(input.readNBytes(parseUInt16(input)))
        case MsgPackKeys.Str32 => new String(input.readNBytes(parseUInt32(input)))
        case x                 => new String(input.readNBytes(x & 0x1f))
      }
    }

    override def unsafeEncode(s: String, output: OutputStream): Unit = {
      val strBytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8)
      val length   = strBytes.length
      if (length <= 31) {
        output.write((MsgPackKeys.FixStrMask | length).toByte)
      } else if (length <= 255) {
        output.write(MsgPackKeys.Str8.toByte)
        writeUInt8(length, output)
      } else if (length <= 65535) {
        output.write(MsgPackKeys.Str16.toByte)
        writeUInt16(length, output)
      } else {
        output.write(MsgPackKeys.Str32.toByte)
        writeUInt32(length, output)
      }
      output.write(strBytes)
    }
  }

//  implicit def option[A](implicit codec1: MsgPackCodec[A]) = new MsgPackCodec[Option[A]] {
//    override def unsafeDecode(input: InputStream): Option[A] = {
//      val n = input.read()
//      (n & 0xff: @switch) match {
//        case MsgPackKeys.Nil => None
//        case x               => input.readNBytes(x & 0x1f)
//      }
//    }
//
//    override def unsafeEncode(a: Option[A], output: OutputStream): Unit = ???
//  }

  implicit def map3[A1, A2, A3](implicit
    codec1: MsgPackCodec[A1],
    codec2: MsgPackCodec[A2],
    codec3: MsgPackCodec[A3],
    stringCodec: MsgPackCodec[String]
  ) =
    new MsgPackCodec[((String, A1), (String, A2), (String, A3))] {
      override def unsafeDecode(input: InputStream): ((String, A1), (String, A2), (String, A3)) = {
        val n = input.read()
        if (n == (MsgPackKeys.FixMapMask | 3)) {
          (
            stringCodec.unsafeDecode(input) -> codec1.unsafeDecode(input),
            stringCodec.unsafeDecode(input) -> codec2.unsafeDecode(input),
            stringCodec.unsafeDecode(input) -> codec3.unsafeDecode(input)
          )
        } else {
          throw new DeserializationTypeError("incorrect marker for Fix Map of 3 " + n.toHexString)
        }
      }

      override def unsafeEncode(a: ((String, A1), (String, A2), (String, A3)), output: OutputStream): Unit = {
        output.write(MsgPackKeys.FixMapMask | 3)
        stringCodec.unsafeEncode(a._1._1, output)
        codec1.unsafeEncode(a._1._2, output)
        stringCodec.unsafeEncode(a._2._1, output)
        codec2.unsafeEncode(a._2._2, output)
        stringCodec.unsafeEncode(a._3._1, output)
        codec3.unsafeEncode(a._3._2, output)
      }

    }

  implicit def map7[A1, A2, A3, A4, A5, A6, A7](implicit
    codec1: MsgPackCodec[A1],
    codec2: MsgPackCodec[A2],
    codec3: MsgPackCodec[A3],
    codec4: MsgPackCodec[A4],
    codec5: MsgPackCodec[A5],
    codec6: MsgPackCodec[A6],
    codec7: MsgPackCodec[A7],
    stringCodec: MsgPackCodec[String]
  ) =
    new MsgPackCodec[
      ((String, A1), (String, A2), (String, A3), (String, A4), (String, A5), (String, A6), (String, A7))
    ] {
      override def unsafeDecode(
        input: InputStream
      ): ((String, A1), (String, A2), (String, A3), (String, A4), (String, A5), (String, A6), (String, A7)) = {
        val n = input.read()
        if (n == (MsgPackKeys.FixMapMask | 7)) {
          (
            stringCodec.unsafeDecode(input) -> codec1.unsafeDecode(input),
            stringCodec.unsafeDecode(input) -> codec2.unsafeDecode(input),
            stringCodec.unsafeDecode(input) -> codec3.unsafeDecode(input),
            stringCodec.unsafeDecode(input) -> codec4.unsafeDecode(input),
            stringCodec.unsafeDecode(input) -> codec5.unsafeDecode(input),
            stringCodec.unsafeDecode(input) -> codec6.unsafeDecode(input),
            stringCodec.unsafeDecode(input) -> codec7.unsafeDecode(input)
          )
        } else {
          throw new DeserializationTypeError("incorrect marker for Fix Map of 7 " + n.toHexString)
        }
      }

      override def unsafeEncode(
        a: ((String, A1), (String, A2), (String, A3), (String, A4), (String, A5), (String, A6), (String, A7)),
        output: OutputStream
      ): Unit = {
        output.write(MsgPackKeys.FixMapMask | 7)
        stringCodec.unsafeEncode(a._1._1, output)
        codec1.unsafeEncode(a._1._2, output)
        stringCodec.unsafeEncode(a._2._1, output)
        codec2.unsafeEncode(a._2._2, output)
        stringCodec.unsafeEncode(a._3._1, output)
        codec3.unsafeEncode(a._3._2, output)
        stringCodec.unsafeEncode(a._4._1, output)
        codec4.unsafeEncode(a._4._2, output)
        stringCodec.unsafeEncode(a._4._1, output)
        codec5.unsafeEncode(a._5._2, output)
        stringCodec.unsafeEncode(a._4._1, output)
        codec6.unsafeEncode(a._6._2, output)
        stringCodec.unsafeEncode(a._4._1, output)
        codec7.unsafeEncode(a._7._2, output)
      }

    }

  implicit val boolean: MsgPackCodec[Boolean] = new MsgPackCodec[Boolean] {
    override def unsafeDecode(input: InputStream): Boolean = {
      val n = input.read()
      (n & 0xff: @switch) match {
        case MsgPackKeys.True  => true
        case MsgPackKeys.False => false
      }
    }

    override def unsafeEncode(a: Boolean, output: OutputStream): Unit =
      if (a) {
        output.write(MsgPackKeys.True)
      } else {
        output.write(MsgPackKeys.False)
      }
  }

  implicit val int: MsgPackCodec[Int] = new MsgPackCodec[Int] {
    override def unsafeDecode(input: InputStream): Int = {
      val n = input.read()
      (n & 0xff: @switch) match {
        case MsgPackKeys.Int32  => parseUInt32(input)
        case MsgPackKeys.UInt16 => parseUInt16(input)
        case MsgPackKeys.Int16  => parseUInt16(input)
        case MsgPackKeys.UInt8  => input.read()
        case x                  => x
      }
    }

    override def unsafeEncode(i: Int, output: OutputStream): Unit =
      if (i >= 0) {
        if (i <= 127) output.write(i)
        else if (i <= 255) {
          output.write(MsgPackKeys.UInt8.toByte)
          output.write(i.toByte)
        } else if (i <= Short.MaxValue) {
          output.write(MsgPackKeys.Int16.toByte)
          writeUInt16(i, output)
        } else if (i <= 0xffff) {
          output.write(MsgPackKeys.UInt16.toByte)
          writeUInt16(i, output)
        } else {
          output.write(MsgPackKeys.Int32.toByte)
          writeUInt32(i, output)
        }
      } else {
        if (i >= -32) output.write(i | 0xe0)
        else if (i >= -128) {
          output.write(MsgPackKeys.Int8.toByte)
          output.write(i.toByte)
        } else if (i >= Short.MinValue) {
          output.write(MsgPackKeys.Int16.toByte)
          writeUInt16(i, output)
        } else {
          output.write(MsgPackKeys.Int32.toByte)
          writeUInt32(i, output)
        }
      }
  }
}
