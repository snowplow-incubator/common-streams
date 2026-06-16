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

import cats.data.NonEmptyList
import cats.syntax.option._
import cats.effect.testing.specs2.CatsEffect
import cats.effect.IO
import cats.effect.kernel.Unique
import fs2.{Chunk, Stream}
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure => BadRowFailure, Payload => BadRowPayload, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.streams.compression.Decompression._

import com.snowplowanalytics.snowplow.streams.compression.CompressionTestUtils._

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Base64

class PayloadProviderSpec extends Specification with CatsEffect {
  import PayloadProviderSpec._

  def is = s2"""
  PayloadProvider should
    handle uncompressed payloads only                                     $handleUncompressedOnly
    handle ZSTD compressed payloads only                                  $handleZstdOnly
    handle GZIP compressed payloads only                                  $handleGzipOnly
    handle mixture of compressed and uncompressed payloads                $handleMixedPayloads
    handle empty input                                                    $handleEmptyInput
    emit multiple batches when decompressed size exceeds maxBytesInBatch  $handleBatchSizeLimits
    reject payloads exceeding maxBytesSinglePayload size limit            $handlePayloadSizeLimit
    handle corrupt compressed data                                        $handleCorruptCompression
    handle unsupported compression header versions                        $handleUnsupportedVersions
    handle malformed compression signatures                               $handleMalformedSignatures
    handle very small buffers that don't match compression signatures     $handleSmallBuffers
    handle alternating valid and invalid compressed messages              $handleAlternatingValidInvalid
    handle large number of small payloads                                 $handleLargeNumberSmallPayloads
    handle zero-length payloads                                           $handleZeroLengthPayloads
    set payloadVersion to None for uncompressed payloads                  $payloadVersionNoneForUncompressed
    set payloadVersion to Some for compressed payloads                    $payloadVersionSomeForCompressed
  """

  def handleUncompressedOnly: IO[MatchResult[Any]] = {
    val payloads = List("payload1", "payload2", "payload3").map(s => ByteBuffer.wrap(s.getBytes))

    createTokenedEvents(payloads).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 3) and
          (results.head.payloads must containTheSameElementsAs(payloads)) and
          (results.head.bad must beEmpty) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some)) and
          (results.head.payloadVersion must beNone)
      }
    }
  }

  def handleZstdOnly: IO[MatchResult[Any]] =
    testSingleCompressionType(List("zstd payload 1", "zstd payload 2"), createZstdCompressedStream(_))

  def handleGzipOnly: IO[MatchResult[Any]] =
    testSingleCompressionType(List("gzip payload 1", "gzip payload 2"), createGzipCompressedStream(_))

  private def testSingleCompressionType(originalPayloads: List[String], compressionFn: List[String] => ByteBuffer): IO[MatchResult[Any]] = {
    val compressedPayload = compressionFn(originalPayloads)

    createTokenedEvents(List(compressedPayload)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 2) and
          (results.head.payloads.map(extractString) must containTheSameElementsAs(originalPayloads)) and
          (results.head.bad must beEmpty) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some)) and
          (results.head.payloadVersion must beSome(1))
      }
    }
  }

  def handleMixedPayloads: IO[MatchResult[Any]] = {
    val uncompressedPayloads = List("uncompressed1", "uncompressed2").map(s => ByteBuffer.wrap(s.getBytes))
    val zstdPayloads         = List("zstd1", "zstd2")
    val gzipPayloads         = List("gzip1", "gzip2")

    val compressedZstd = createZstdCompressedStream(zstdPayloads)
    val compressedGzip = createGzipCompressedStream(gzipPayloads)

    val allPayloads = uncompressedPayloads ++ List(compressedZstd, compressedGzip)

    createTokenedEvents(allPayloads).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 3) and
          (results(0).payloads must have length 2) and
          (results(0).payloads must containTheSameElementsAs(uncompressedPayloads)) and
          (results(0).bad must beEmpty) and
          (results(0).ack must beNone) and
          (results(0).payloadVersion must beNone) and
          (results(1).payloads must have length 2) and
          (results(1).payloads.map(extractString) must containTheSameElementsAs(zstdPayloads)) and
          (results(1).bad must beEmpty) and
          (results(1).ack must beNone) and
          (results(1).payloadVersion must beSome(1)) and
          (results(2).payloads must have length 2) and
          (results(2).payloads.map(extractString) must containTheSameElementsAs(gzipPayloads)) and
          (results(2).bad must beEmpty) and
          (results(2).ack must beEqualTo(tokenedEvents.ack.some)) and
          (results(2).payloadVersion must beSome(1))
      }
    }
  }

  def handleEmptyInput: IO[MatchResult[Any]] =
    createTokenedEvents(List.empty).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must beEmpty) and
          (results.head.bad must beEmpty) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }

  def handleBatchSizeLimits: IO[MatchResult[Any]] = {
    val largePayload1 = "x" * 300 // Each payload is 300 bytes
    val payloads      = (1 to 20).map(_ => largePayload1).toList // Total would be 6000 bytes, exceeding 2000 byte limit

    val compressedPayload = createZstdCompressedStream(payloads)

    createTokenedEvents(List(compressedPayload)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 3) and // Should split into 3 batches
          (results(0).payloads must have length 7) and // First batch gets first payload
          (results(0).bad must beEmpty) and
          (results(0).ack must beNone) and // First batch should not have ack token
          (results(1).payloads must have length 7) and // Second batch gets second payload
          (results(1).bad must beEmpty) and
          (results(1).ack must beNone) and // Second batch should not have ack token
          (results(2).payloads must have length 6) and // Third batch gets third payload
          (results(2).bad must beEmpty) and
          (results(2).ack must beSome) // Third batch should have ack token
      }
    }
  }

  def handlePayloadSizeLimit: IO[MatchResult[Any]] = {
    val oversizedPayload = "x" * 1500 // Exceeds maxBytesSinglePayload (500)
    val validPayload     = "valid payload"

    val compressedPayload = createZstdCompressedStream(List(oversizedPayload, validPayload))

    createTokenedEvents(List(compressedPayload)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 1) and // Only valid payload
          (results.head.payloads.map(extractString) must contain(validPayload)) and
          (results.head.bad must have length 1) and // One bad row for oversized payload
          (results.head.ack must beEqualTo(tokenedEvents.ack.some)) and {
            results.head.bad.head must beAnInstanceOf[BadRow.SizeViolation]
          }
      }
    }
  }

  def handleCorruptCompression: IO[MatchResult[Any]] =
    testDecompressionFailure(
      input           = createIncompleteCompressedStream(CompressionType.ZSTD, false),
      expectedMessage = "corrupt zstd-compressed payload"
    )

  def handleUnsupportedVersions: IO[MatchResult[Any]] =
    testDecompressionFailure(
      input           = createZstdCompressedStreamWithVersions(List("test"), compressionVersion = 99, payloadVersion = 1),
      expectedMessage = "unsupported versions in zstd-compressed record header: 99, 1"
    )

  private def testDecompressionFailure(input: ByteBuffer, expectedMessage: String): IO[MatchResult[Any]] = {
    val expectedBase64 = bufferAsBase64(input)

    createTokenedEvents(List(input)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig, toBadRow).map { results =>
        val failure = results.head.bad.head.asInstanceOf[BadRow.GenericError].failure
        ((results must have length 1): MatchResult[Any]) and
          (results.head.payloads must beEmpty) and
          (results.head.bad must have length 1) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some)) and
          (failure.errors.toList must beEqualTo(List(expectedMessage))) and
          (results.head.bad.head.asInstanceOf[BadRow.GenericError].payload.event must beEqualTo(expectedBase64))
      }
    }
  }

  def handleMalformedSignatures: IO[MatchResult[Any]] = {
    // Create buffer that starts with partial ZSTD signature but isn't actually compressed
    val malformedBuffer = ByteBuffer.wrap(Array[Int](0x28, 0xb5, 0x2f).map(_.toByte)) // Incomplete ZSTD signature
    val payloads        = List(malformedBuffer)

    createTokenedEvents(payloads).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 1) and // Treated as uncompressed
          (results.head.payloads must containTheSameElementsAs(payloads)) and
          (results.head.bad must beEmpty) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }
  }

  def handleSmallBuffers: IO[MatchResult[Any]] = {
    val tinyBuffer = ByteBuffer.wrap(Array[Byte](0x01)) // Single byte, too small to match any signature
    val payloads   = List(tinyBuffer)

    createTokenedEvents(payloads).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 1) and // Treated as uncompressed
          (results.head.payloads must containTheSameElementsAs(payloads)) and
          (results.head.bad must beEmpty) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }
  }

  def handleAlternatingValidInvalid: IO[MatchResult[Any]] = {
    val validPayload   = createZstdCompressedStream(List("valid"))
    val corruptPayload = createIncompleteCompressedStream(CompressionType.ZSTD, false)
    val validPayload2  = createZstdCompressedStream(List("valid2"))

    createTokenedEvents(List(validPayload, corruptPayload, validPayload2)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 2) and // Two valid payloads
          (results.head.payloads.map(extractString) must containTheSameElementsAs(List("valid", "valid2"))) and
          (results.head.bad must have length 1) and // One corrupt payload
          (results.head.ack must beEqualTo(tokenedEvents.ack.some)) and {
            results.head.bad.head must beAnInstanceOf[BadRow.GenericError]
          }
      }
    }
  }

  def handleLargeNumberSmallPayloads: IO[MatchResult[Any]] = {
    val payloads          = (1 to 50).map(i => s"small$i").toList
    val compressedPayload = createZstdCompressedStream(payloads)

    createTokenedEvents(List(compressedPayload)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 50) and
          (results.head.payloads.map(extractString) must containTheSameElementsAs(payloads)) and
          (results.head.bad must beEmpty) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }
  }

  def handleZeroLengthPayloads: IO[MatchResult[Any]] = {
    val payloads          = List("", "content", "") // Mix of empty and non-empty
    val compressedPayload = createZstdCompressedStream(payloads)

    createTokenedEvents(List(compressedPayload)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 3) and
          (results.head.payloads.map(extractString) must containTheSameElementsAs(payloads)) and
          (results.head.bad must beEmpty) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }
  }

  def payloadVersionNoneForUncompressed: IO[MatchResult[Any]] = {
    val payloads = List("payload1").map(s => ByteBuffer.wrap(s.getBytes))
    createTokenedEvents(payloads).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        results.head.payloadVersion must beNone
      }
    }
  }

  def payloadVersionSomeForCompressed: IO[MatchResult[Any]] = {
    val compressedPayload = createZstdCompressedStream(List("test"), payloadVersion = 42)
    createTokenedEvents(List(compressedPayload)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        results.head.payloadVersion must beSome(42)
      }
    }
  }

  private def createTokenedEvents(payloads: List[ByteBuffer]): IO[TokenedEvents] =
    Unique[IO].unique.map(t => TokenedEvents(Chunk.from(payloads), t))

  private def runPayloadProvider(
    tokenedEvents: TokenedEvents,
    config: DecompressionConfig,
    toBadRow: DecompressionError => BadRow = sentinelDecompressionBadRow(testBadRowProcessor)
  ): IO[List[DecompressedTokenedEvents]] =
    Stream
      .emit[IO, TokenedEvents](tokenedEvents)
      .through(PayloadProvider.pipe(testBadRowProcessor, config, toBadRow))
      .compile
      .toList

}

object PayloadProviderSpec {
  val testConfig = DecompressionConfig(maxBytesInBatch = 2000, maxBytesSinglePayload = 500)

  val testBadRowProcessor = BadRowProcessor("test-app", "test-version")

  def createZstdCompressedStream(
    payloads: List[String],
    compressionVersion: Byte = 1,
    payloadVersion: Byte     = 1
  ): ByteBuffer =
    createCompressedStream(payloads.map(_.getBytes), CompressionType.ZSTD, compressionVersion, payloadVersion)

  def createGzipCompressedStream(
    payloads: List[String],
    compressionVersion: Byte = 1,
    payloadVersion: Byte     = 1
  ): ByteBuffer =
    createCompressedStream(payloads.map(_.getBytes), CompressionType.GZIP, compressionVersion, payloadVersion)

  def createZstdCompressedStreamWithVersions(
    payloads: List[String],
    compressionVersion: Int,
    payloadVersion: Int
  ): ByteBuffer =
    createCompressedStream(payloads.map(_.getBytes), CompressionType.ZSTD, compressionVersion.toByte, payloadVersion.toByte)

  /**
   * Builds a `GenericError` that surfaces the `DecompressionError`'s fields verbatim, so tests can
   * assert on them by reading the resulting bad row.
   */
  val toBadRow: DecompressionError => BadRow =
    error =>
      BadRow.GenericError(
        testBadRowProcessor,
        BadRowFailure.GenericFailure(error.timestamp, NonEmptyList.of(error.message)),
        BadRowPayload.RawPayload(error.payload)
      )

  def bufferAsBase64(bb: ByteBuffer): String =
    StandardCharsets.UTF_8.decode(Base64.getEncoder.encode(bb.slice())).toString
}
