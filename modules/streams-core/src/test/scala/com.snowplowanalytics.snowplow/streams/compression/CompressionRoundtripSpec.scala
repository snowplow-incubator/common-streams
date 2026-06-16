/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams.compression

import org.scalacheck.Gen
import org.specs2.ScalaCheck
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import com.snowplowanalytics.snowplow.streams.compression.CompressionTestUtils.TestPayloadVersion

import scala.annotation.tailrec
import scala.util.Random

class CompressionRoundtripSpec extends Specification with ScalaCheck {

  def is = s2"""
  Compress/Decompress roundtrip should
  ${roundtripTests("zstd", zstdCompressor, zstdFactory)}
  ${roundtripTests("gzip", gzipCompressor, gzipFactory)}
  """

  private def roundtripTests(
    compressionType: String,
    cf: Compressor.Factory,
    df: Decompressor.Factory
  ) = {
    def singleRecord      = roundtrip(cf, df, List("hello world".getBytes("UTF-8")))
    def multipleRecs      = roundtrip(cf, df, multipleRecords)
    def emptyRecord       = roundtrip(cf, df, List(Array.empty[Byte]))
    def binaryData        = roundtrip(cf, df, binaryRecords)
    def unicode           = roundtrip(cf, df, unicodeRecords)
    def nullBytes         = roundtrip(cf, df, nullByteRecords)
    def largePayload      = largePayloadRoundtrip(cf, df)
    def mixedSizes        = roundtrip(cf, df, mixedSizeRecords)
    def manySmall         = roundtrip(cf, df, manySmallRecords)
    def payloadVersion    = payloadVersionRoundtrip(cf, df)
    def recordCount       = recordCountRoundtrip(cf, df, fiveRecords)
    def partialAcceptance = partialAcceptanceRoundtrip(cf, df)
    def singleByte        = roundtrip(cf, df, List(Array[Byte](42)))
    def highBitBytes      = roundtrip(cf, df, List(Array.tabulate[Byte](256)(i => i.toByte)))
    def randomRecords = prop { (input: (List[Array[Byte]], Int)) =>
      roundtripProp(cf, df, input._1, input._2)
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

  private val zstdCompressor = ZstdCompressor.factory(3)
  private val gzipCompressor = GzipCompressor.factory(6)
  private val zstdFactory    = new Decompressor.Zstd(Int.MaxValue)
  private val gzipFactory    = new Decompressor.Gzip(Int.MaxValue)

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
    List("日本語テスト", "e\u0301 combine\u0301", "emoji: 🎉🚀", "mixed: abc日本語def").map(_.getBytes("UTF-8"))

  private val genRecord: Gen[Array[Byte]] =
    Gen.chooseNum(1, 1000).flatMap(Gen.listOfN(_, Gen.choose[Byte](Byte.MinValue, Byte.MaxValue))).map(_.toArray)
  private val genRecordsWithTargetSize: Gen[(List[Array[Byte]], Int)] = for {
    records <- Gen.chooseNum(1, 10).flatMap(Gen.listOfN(_, genRecord))
    targetSize <- Gen.chooseNum(100, 10000)
  } yield (records, targetSize)

  private def roundtripProp(
    compressorFactory: Compressor.Factory,
    decompressorFactory: Decompressor.Factory,
    records: List[Array[Byte]],
    targetSize: Int
  ): Boolean = {
    val compressor = compressorFactory.buildAndInitialize(targetSize, TestPayloadVersion)
    val accepted   = records.takeWhile(r => compressor.addRecord(r, 0, r.length))

    // If nothing fits, there is no roundtrip
    if (accepted.isEmpty) return true

    val compressed = compressor.result

    decompressorFactory.build(compressed) match {
      case Decompressor.FactorySuccess(decompressor, pv) =>
        val decompressed = drainRecords(decompressor)
        pv == TestPayloadVersion &&
        decompressed.length == accepted.length &&
        decompressed.zip(accepted).forall { case (actual, expected) => actual.sameElements(expected) }
      case _ => false
    }
  }

  // 100KB of repeated bytes compresses very well; targetSize=200000 provides generous headroom
  // to ensure the test exercises large payload handling rather than hitting the size limit
  private def largePayloadRoundtrip(cf: Compressor.Factory, df: Decompressor.Factory) =
    roundtrip(cf, df, List(("x" * 100000).getBytes("UTF-8")), targetSize = 200000)

  private def roundtrip(
    compressorFactory: Compressor.Factory,
    decompressorFactory: Decompressor.Factory,
    records: List[Array[Byte]],
    targetSize: Int = 50000
  ): MatchResult[Any] = {
    val compressor = compressorFactory.buildAndInitialize(targetSize, TestPayloadVersion)
    records.foreach(r => compressor.addRecord(r, 0, r.length))
    val compressed = compressor.result

    decompressorFactory.build(compressed) must beLike { case Decompressor.FactorySuccess(decompressor, pv) =>
      val decompressed = drainRecords(decompressor)

      (pv must beEqualTo(TestPayloadVersion)) and
        (decompressed.length must beEqualTo(records.length)) and
        (decompressed.zip(records).forall { case (actual, expected) => actual.sameElements(expected) } must beTrue)
    }
  }

  private def payloadVersionRoundtrip(
    compressorFactory: Compressor.Factory,
    decompressorFactory: Decompressor.Factory
  ): MatchResult[Any] = {
    val compressor = compressorFactory.buildAndInitialize(1000, TestPayloadVersion)
    val record     = "test".getBytes("UTF-8")
    val _          = compressor.addRecord(record, 0, record.length)
    val compressed = compressor.result

    decompressorFactory.build(compressed) must beLike { case Decompressor.FactorySuccess(decompressor, pv) =>
      decompressor.close()
      pv must beEqualTo(TestPayloadVersion)
    }
  }

  private def recordCountRoundtrip(
    compressorFactory: Compressor.Factory,
    decompressorFactory: Decompressor.Factory,
    records: List[Array[Byte]]
  ): MatchResult[Any] = {
    val compressor = compressorFactory.buildAndInitialize(50000, TestPayloadVersion)
    records.foreach(r => compressor.addRecord(r, 0, r.length))

    val count      = compressor.recordCount
    val compressed = compressor.result

    decompressorFactory.build(compressed) must beLike { case Decompressor.FactorySuccess(decompressor, _) =>
      val decompressed = drainRecords(decompressor)

      (count must beEqualTo(records.length)) and
        (decompressed.length must beEqualTo(count))
    }
  }

  private def partialAcceptanceRoundtrip(
    compressorFactory: Compressor.Factory,
    decompressorFactory: Decompressor.Factory
  ): MatchResult[Any] = {
    val compressor = compressorFactory.buildAndInitialize(1000, TestPayloadVersion)
    val small1     = "small1".getBytes("UTF-8")
    val small2     = "small2".getBytes("UTF-8")
    val random     = new Random(99)
    val large      = new Array[Byte](10000)
    random.nextBytes(large) // random bytes don't compress well

    val r1         = compressor.addRecord(small1, 0, small1.length)
    val r2         = compressor.addRecord(small2, 0, small2.length)
    val r3         = compressor.addRecord(large, 0, large.length)
    val compressed = compressor.result

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
