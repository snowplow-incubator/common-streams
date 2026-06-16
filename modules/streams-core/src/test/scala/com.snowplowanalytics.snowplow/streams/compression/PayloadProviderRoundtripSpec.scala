/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams
package compression

import cats.effect.IO
import cats.effect.kernel.Unique
import cats.effect.testing.specs2.CatsEffect
import cats.effect.unsafe.implicits.global
import fs2.{Chunk, Stream}
import org.scalacheck.Gen
import org.specs2.ScalaCheck
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import com.snowplowanalytics.snowplow.badrows.{Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.streams.compression.CompressionTestUtils.{
  TestPayloadVersion,
  extractString,
  sentinelDecompressionBadRow
}

import java.nio.ByteBuffer

class PayloadProviderRoundtripSpec extends Specification with CatsEffect with ScalaCheck {

  def is = s2"""
  Compressor to PayloadProvider roundtrip should
    preserve single record with zstd                         $singleRecordZstd
    preserve single record with gzip                         $singleRecordGzip
    preserve multiple records with zstd                      $multipleRecordsZstd
    preserve multiple records with gzip                      $multipleRecordsGzip
    preserve unicode strings with zstd                       $unicodeZstd
    preserve unicode strings with gzip                       $unicodeGzip
    preserve payload version with zstd                       $payloadVersionZstd
    preserve payload version with gzip                       $payloadVersionGzip
    preserve many small records with zstd                    $manySmallRecordsZstd
    preserve many small records with gzip                    $manySmallRecordsGzip
    handle mixed compressed and uncompressed in one batch    $mixedCompressedAndUncompressed
    split into multiple batches when decompressed size exceeds maxBytesInBatch  $batchSplitting
    preserve random payloads with zstd                       $randomPayloadsZstd
    preserve random payloads with gzip                       $randomPayloadsGzip
  """

  private def singleRecordZstd = zstdProviderRoundtrip(List("hello world"))
  private def singleRecordGzip = gzipProviderRoundtrip(List("hello world"))

  private def multipleRecordsZstd = zstdProviderRoundtrip(List("first", "second", "third"))
  private def multipleRecordsGzip = gzipProviderRoundtrip(List("first", "second", "third"))

  private def unicodeZstd =
    zstdProviderRoundtrip(List("日本語テスト", "e\u0301 combine\u0301", "emoji: 🎉🚀", "mixed: abc日本語def"))
  private def unicodeGzip =
    gzipProviderRoundtrip(List("日本語テスト", "e\u0301 combine\u0301", "emoji: 🎉🚀", "mixed: abc日本語def"))

  private def payloadVersionZstd = zstdProviderRoundtrip(List("test"), payloadVersion = 99)
  private def payloadVersionGzip = gzipProviderRoundtrip(List("test"), payloadVersion = 99)

  private def manySmallRecordsZstd = zstdProviderRoundtrip((1 to 100).map(i => s"record-$i").toList)
  private def manySmallRecordsGzip = gzipProviderRoundtrip((1 to 100).map(i => s"record-$i").toList)

  private def mixedCompressedAndUncompressed: IO[MatchResult[Any]] = {
    val uncompressedPayload = "raw-payload"
    val compressedPayloads  = List("compressed1", "compressed2")

    val compressor = ZstdCompressor.factory(3).buildAndInitialize(50000, TestPayloadVersion)
    compressedPayloads.foreach { s =>
      val bytes = s.getBytes("UTF-8")
      compressor.addRecord(bytes, 0, bytes.length)
    }
    val compressedBuffer = compressor.result

    val allPayloads = List(ByteBuffer.wrap(uncompressedPayload.getBytes("UTF-8")), compressedBuffer)

    Unique[IO].unique.map(t => TokenedEvents(Chunk.from(allPayloads), t)).flatMap { tokenedEvents =>
      Stream
        .emit[IO, TokenedEvents](tokenedEvents)
        .through(PayloadProvider.pipe(testBadRowProcessor, testConfig, unusedToBadRow))
        .compile
        .toList
        .map { results =>
          val allPayloadStrings = results.flatMap(_.payloads).map(extractString)
          val allBad            = results.flatMap(_.bad)

          (allPayloadStrings must containTheSameElementsAs(List(uncompressedPayload) ++ compressedPayloads)) and
            (allBad must beEmpty)
        }
    }
  }

  private def batchSplitting: IO[MatchResult[Any]] = {
    // 20 records of 300 bytes each = 6000 bytes decompressed, with maxBytesInBatch=2000.
    // This must produce multiple batches. Only the last batch should carry the ack token.
    val payloads   = (1 to 20).map(_ => "x" * 300).toList
    val compressor = ZstdCompressor.factory(3).buildAndInitialize(50000, TestPayloadVersion)
    payloads.foreach { s =>
      val bytes = s.getBytes("UTF-8")
      compressor.addRecord(bytes, 0, bytes.length)
    }
    val compressed  = compressor.result
    val smallConfig = DecompressionConfig(maxBytesInBatch = 2000, maxBytesSinglePayload = 50000)

    Unique[IO].unique.map(t => TokenedEvents(Chunk.singleton(compressed), t)).flatMap { tokenedEvents =>
      Stream
        .emit[IO, TokenedEvents](tokenedEvents)
        .through(PayloadProvider.pipe(testBadRowProcessor, smallConfig, unusedToBadRow))
        .compile
        .toList
        .map { results =>
          val allPayloads = results.flatMap(_.payloads).map(extractString)
          val allBad      = results.flatMap(_.bad)

          (results.length must beGreaterThan(1): MatchResult[Any]) and
            (allPayloads.sorted must beEqualTo(payloads.sorted)) and
            (allBad must beEmpty) and
            (results.init.forall(_.ack.isEmpty) must beTrue) and
            (results.last.ack must beSome)
        }
    }
  }

  private val genPayload: Gen[String]        = Gen.chooseNum(1, 1000).flatMap(Gen.listOfN(_, Gen.alphaNumChar)).map(_.mkString)
  private val genPayloads: Gen[List[String]] = Gen.chooseNum(1, 10).flatMap(Gen.listOfN(_, genPayload))

  private def randomPayloadsZstd = prop { (payloads: List[String]) =>
    zstdProviderRoundtrip(payloads).unsafeRunSync()
  }.setGen(genPayloads)

  private def randomPayloadsGzip = prop { (payloads: List[String]) =>
    gzipProviderRoundtrip(payloads).unsafeRunSync()
  }.setGen(genPayloads)

  private val testBadRowProcessor = BadRowProcessor("test-app", "test-version")
  private val testConfig          = DecompressionConfig(maxBytesInBatch = 100000, maxBytesSinglePayload = 50000)

  private val unusedToBadRow = sentinelDecompressionBadRow(testBadRowProcessor)

  private def zstdProviderRoundtrip(payloads: List[String], payloadVersion: Int = TestPayloadVersion) =
    providerRoundtrip(ZstdCompressor.factory(3), payloads, payloadVersion = payloadVersion)

  private def gzipProviderRoundtrip(payloads: List[String], payloadVersion: Int = TestPayloadVersion) =
    providerRoundtrip(GzipCompressor.factory(6), payloads, payloadVersion = payloadVersion)

  private def providerRoundtrip(
    compressorFactory: Compressor.Factory,
    originalPayloads: List[String],
    payloadVersion: Int
  ): IO[MatchResult[Any]] = {
    val compressor = compressorFactory.buildAndInitialize(50000, payloadVersion)
    originalPayloads.foreach { s =>
      val bytes = s.getBytes("UTF-8")
      compressor.addRecord(bytes, 0, bytes.length)
    }
    val compressed = compressor.result

    Unique[IO].unique.map(t => TokenedEvents(Chunk.singleton(compressed), t)).flatMap { tokenedEvents =>
      Stream
        .emit[IO, TokenedEvents](tokenedEvents)
        .through(PayloadProvider.pipe(testBadRowProcessor, testConfig, unusedToBadRow))
        .compile
        .toList
        .map { results =>
          val allPayloads = results.flatMap(_.payloads).map(extractString)
          val allBad      = results.flatMap(_.bad)
          val pv          = results.flatMap(_.payloadVersion).headOption

          (allPayloads must containTheSameElementsAs(originalPayloads)) and
            (allBad must beEmpty) and
            (pv must beSome(payloadVersion)) and
            (results.last.ack must beSome(tokenedEvents.ack))
        }
    }
  }

}
