/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams.compression

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import cats.effect.unsafe.implicits.global
import org.scalacheck.Gen
import org.specs2.ScalaCheck
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import com.snowplowanalytics.snowplow.streams.compression.CompressionTestUtils.TestPayloadVersion

import java.nio.ByteBuffer
import scala.annotation.tailrec
import scala.util.Random

class CompressionRoundtripSpec extends Specification with CatsEffect with ScalaCheck {

  def is = s2"""
  Compress/Decompress roundtrip should
  ${roundtripTests("zstd", zstdCompressor, zstdFactory)}
  ${roundtripTests("gzip", gzipCompressor, gzipFactory)}
  """

  private def roundtripTests(
    compressionType: String,
    factory: CompressorFactory,
    df: Decompressor.Factory
  ) = {
    def singleRecord      = roundtrip(factory, df, List("hello world".getBytes("UTF-8")))
    def multipleRecs      = roundtrip(factory, df, multipleRecords)
    def emptyRecord       = roundtrip(factory, df, List(Array.empty[Byte]))
    def binaryData        = roundtrip(factory, df, binaryRecords)
    def unicode           = roundtrip(factory, df, unicodeRecords)
    def nullBytes         = roundtrip(factory, df, nullByteRecords)
    def largePayload      = largePayloadRoundtrip(factory, df)
    def mixedSizes        = roundtrip(factory, df, mixedSizeRecords)
    def manySmall         = roundtrip(factory, df, manySmallRecords)
    def payloadVersion    = payloadVersionRoundtrip(factory, df)
    def recordCount       = recordCountRoundtrip(factory, df, fiveRecords)
    def partialAcceptance = partialAcceptanceRoundtrip(factory, df)
    def singleByte        = roundtrip(factory, df, List(Array[Byte](42)))
    def highBitBytes      = roundtrip(factory, df, List(Array.tabulate[Byte](256)(i => i.toByte)))
    def randomRecords = prop { (input: (List[Array[Byte]], Int)) =>
      roundtripProp(factory, df, input._1, input._2)
    }.setGen(genRecordsWithTargetSize)

    s2"""
  $compressionType
    preserve single record                                   $singleRecord
    preserve multiple records                                $multipleRecs
    preserve empty record                                    $emptyRecord
    preserve binary data                                     $binaryData
    preserve unicode strings                                 $unicode
    preserve records with null bytes                         $nullBytes
    preserve large payload                                   $largePayload
    preserve mixed record sizes                              $mixedSizes
    preserve many small records                              $manySmall
    preserve payload version through roundtrip               $payloadVersion
    report correct record count                              $recordCount
    preserve accepted records when last record is rejected   $partialAcceptance
    preserve single-byte record                              $singleByte
    preserve high-bit bytes                                  $highBitBytes
    preserve random records                                  $randomRecords
  """
  }

  private val zstdCompressor: CompressorFactory = CompressorFactory.zstd(3)
  private val gzipCompressor: CompressorFactory = CompressorFactory.gzip(6)
  private val zstdFactory                       = new Decompressor.Zstd(Int.MaxValue)
  private val gzipFactory                       = new Decompressor.Gzip(Int.MaxValue)

  private val multipleRecords = List("first", "second", "third").map(_.getBytes("UTF-8"))
  private val nullByteRecords = List(Array[Byte](0, 0, 0), Array[Byte](1, 0, 2, 0, 3))
  private val mixedSizeRecords =
    List("x", "y" * 100, "z" * 1000, "w" * 10, "v" * 500).map(_.getBytes("UTF-8"))
  private val manySmallRecords = (1 to 200).map(i => s"record-$i".getBytes("UTF-8")).toList
  private val fiveRecords      = List("a", "b", "c", "d", "e").map(_.getBytes("UTF-8"))

  private val binaryRecords = {
    val random = new Random(42)
    (1 to 5).map { _ =>
      val bytes = new Array[Byte](200)
      random.nextBytes(bytes)
      bytes
    }.toList
  }

  private val unicodeRecords =
    List("日本語テスト", "é combiné", "emoji: 🎉🚀", "mixed: abc日本語def").map(_.getBytes("UTF-8"))

  private val genRecord: Gen[Array[Byte]] =
    Gen.chooseNum(1, 1000).flatMap(Gen.listOfN(_, Gen.choose[Byte](Byte.MinValue, Byte.MaxValue))).map(_.toArray)
  private val genRecordsWithTargetSize: Gen[(List[Array[Byte]], Int)] = for {
    records <- Gen.chooseNum(1, 10).flatMap(Gen.listOfN(_, genRecord))
    targetSize <- Gen.chooseNum(100, 10000)
  } yield (records, targetSize)

  // Acquire, run the batch, release — synchronously. Used only inside the ScalaCheck `prop`
  // combinator below, which needs a plain Boolean rather than an IO.
  private def roundtripProp(
    factory: CompressorFactory,
    decompressorFactory: Decompressor.Factory,
    records: List[Array[Byte]],
    targetSize: Int
  ): Boolean =
    factory
      .resource[IO]
      .use { c =>
        IO {
          c.reset(TestPayloadVersion, targetSize)
          val accepted = records.takeWhile(r => c.addRecord(r, 0, r.length))
          if (accepted.isEmpty) true
          else {
            val compressed = c.result
            decompressorFactory.build(compressed) match {
              case Decompressor.FactorySuccess(decompressor, pv) =>
                val decompressed = drainRecords(decompressor)
                pv == TestPayloadVersion &&
                decompressed.length == accepted.length &&
                decompressed.zip(accepted).forall { case (a, e) => a.sameElements(e) }
              case _ => false
            }
          }
        }
      }
      .unsafeRunSync()

  // Acquire, run the batch, release. Returns the compressed result bytes.
  private def compress(
    factory: CompressorFactory,
    targetSize: Int,
    records: List[Array[Byte]]
  ): IO[ByteBuffer] =
    factory.resource[IO].use { c =>
      IO {
        c.reset(TestPayloadVersion, targetSize)
        records.foreach(r => c.addRecord(r, 0, r.length))
        c.result
      }
    }

  // 100KB of repeated bytes compresses very well; targetSize=200000 provides generous headroom
  // to ensure the test exercises large payload handling rather than hitting the size limit
  private def largePayloadRoundtrip(factory: CompressorFactory, df: Decompressor.Factory) =
    roundtrip(factory, df, List(("x" * 100000).getBytes("UTF-8")), targetSize = 200000)

  private def roundtrip(
    factory: CompressorFactory,
    decompressorFactory: Decompressor.Factory,
    records: List[Array[Byte]],
    targetSize: Int = 50000
  ): IO[MatchResult[Any]] =
    compress(factory, targetSize, records).map { compressed =>
      decompressorFactory.build(compressed) must beLike { case Decompressor.FactorySuccess(decompressor, pv) =>
        val decompressed = drainRecords(decompressor)

        (pv must beEqualTo(TestPayloadVersion)) and
          (decompressed.length must beEqualTo(records.length)) and
          (decompressed.zip(records).forall { case (actual, expected) => actual.sameElements(expected) } must beTrue)
      }
    }

  private def payloadVersionRoundtrip(
    factory: CompressorFactory,
    decompressorFactory: Decompressor.Factory
  ): IO[MatchResult[Any]] =
    factory.resource[IO].use { c =>
      IO {
        c.reset(TestPayloadVersion, 1000)
        val record     = "test".getBytes("UTF-8")
        val _          = c.addRecord(record, 0, record.length)
        val compressed = c.result

        decompressorFactory.build(compressed) must beLike { case Decompressor.FactorySuccess(decompressor, pv) =>
          decompressor.close()
          pv must beEqualTo(TestPayloadVersion)
        }
      }
    }

  private def recordCountRoundtrip(
    factory: CompressorFactory,
    decompressorFactory: Decompressor.Factory,
    records: List[Array[Byte]]
  ): IO[MatchResult[Any]] =
    factory.resource[IO].use { c =>
      IO {
        c.reset(TestPayloadVersion, 50000)
        records.foreach(r => c.addRecord(r, 0, r.length))

        val count      = c.recordCount
        val compressed = c.result

        decompressorFactory.build(compressed) must beLike { case Decompressor.FactorySuccess(decompressor, _) =>
          val decompressed = drainRecords(decompressor)

          (count must beEqualTo(records.length)) and
            (decompressed.length must beEqualTo(count))
        }
      }
    }

  private def partialAcceptanceRoundtrip(
    factory: CompressorFactory,
    decompressorFactory: Decompressor.Factory
  ): IO[MatchResult[Any]] =
    factory.resource[IO].use { c =>
      IO {
        c.reset(TestPayloadVersion, 1000)
        val small1 = "small1".getBytes("UTF-8")
        val small2 = "small2".getBytes("UTF-8")
        val random = new Random(99)
        val large  = new Array[Byte](10000)
        random.nextBytes(large) // random bytes don't compress well

        val r1         = c.addRecord(small1, 0, small1.length)
        val r2         = c.addRecord(small2, 0, small2.length)
        val r3         = c.addRecord(large, 0, large.length)
        val compressed = c.result

        decompressorFactory.build(compressed) must beLike { case Decompressor.FactorySuccess(decompressor, _) =>
          val decompressed = drainRecords(decompressor)

          (r1 must beTrue) and
            (r2 must beTrue) and
            (r3 must beFalse) and
            (decompressed.length must beEqualTo(2): MatchResult[Any]) and
            (decompressed(0).sameElements(small1) must beTrue) and
            (decompressed(1).sameElements(small2) must beTrue)
        }
      }
    }

  @tailrec
  private def drainRecords(decompressor: Decompressor, acc: List[Array[Byte]] = Nil): List[Array[Byte]] =
    decompressor.getNextRecord match {
      case Decompressor.Record(bytes) => drainRecords(decompressor, bytes :: acc)
      case Decompressor.EndOfRecords =>
        decompressor.close()
        acc.reverse
      case other =>
        decompressor.close()
        throw new RuntimeException(s"Unexpected result: $other")
    }
}
