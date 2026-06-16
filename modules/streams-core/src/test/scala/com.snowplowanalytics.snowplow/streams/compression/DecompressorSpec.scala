/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams.compression

import cats.effect.testing.specs2.CatsEffect
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import scala.util.Random

import com.snowplowanalytics.snowplow.streams.compression.Decompressor._
import com.snowplowanalytics.snowplow.streams.compression.CompressionTestUtils._

class DecompressorSpec extends Specification with CatsEffect {

  def is = s2"""
  Decompressor should
    decompress single record from compressed streams                             $extractSingleRecord
    decompress multiple records from compressed streams                          $extractMultipleRecords
    reject record exceeding max allowed byte limit and continue with next        $exceedMaxByteLimitAndContinue
    continue to return EndOfRecords after first EndOfRecords return              $continueToReturnEndOfRecords
    handle unsupported header versions                                           $ensureUnsupportedVersionsRejected
    handle payload version passed through FactorySuccess                         $ensurePayloadVersionPassedThrough
    handle corrupt input with incomplete size header                             $ensureIncompleteSize
    handle corrupt input with incomplete record data                             $ensureIncompleteRecord
    handle empty compressed stream after header                                  $ensureEmptyStreamHandled
    handle zero-length record                                                    $ensureZeroLengthRecordSupported
    handle very large number of small records                                    $ensureLargeVolumeProcessing
    handle mixed record sizes                                                    $ensureMixedSizesSupported
    handle corrupted zstd payload with trailing null bytes                       $ensureCorruptedZstdWithTrailingNull
    handle gzip stream with zero records                                         $ensureGzipZeroCrcCorrupt
    handle zstd stream with zero records                                         $ensureZstdZeroRecords
  """

  def extractSingleRecord = {
    val record = "test record data"
    testBothCompressionTypes { compressionType =>
      val compressed = createCompressedStream(List(record.getBytes("UTF-8")), compressionType)
      val factory    = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike { case FactorySuccess(decompressor, payloadVersion) =>
        (decompressor.getNextRecord must beLike { case Record(data) =>
          new String(data, "UTF-8") must beEqualTo(record)
        }) and (payloadVersion must beEqualTo(1))
      }
    }
  }

  def extractMultipleRecords = {
    val expectedStrings = List("first record", "second record", "third record")
    val records         = expectedStrings.map(_.getBytes("UTF-8"))
    testBothCompressionTypes { compressionType =>
      validateRecordsInOrder(records, compressionType)
    }
  }

  def exceedMaxByteLimitAndContinue = {
    val smallRecord1        = "small record"
    val smallRecord2        = "another small record"
    val oversizedRecordSize = 2000
    val oversizedRecord     = new Array[Byte](oversizedRecordSize)
    val records = List(
      smallRecord1.getBytes("UTF-8"),
      oversizedRecord,
      smallRecord2.getBytes("UTF-8")
    )
    Random.nextBytes(oversizedRecord)
    testBothCompressionTypes { compressionType =>
      val compressed = createCompressedStream(records, compressionType)
      val factory    = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike { case FactorySuccess(decompressor, _) =>
        val results = List(
          decompressor.getNextRecord,
          decompressor.getNextRecord,
          decompressor.getNextRecord,
          decompressor.getNextRecord
        )
        (results(0) must beLike { case Record(r1) => new String(r1, "UTF-8") must beEqualTo(smallRecord1) }) and
          (results(1) must beLike { case RecordTooBig(size) => size must beEqualTo(oversizedRecordSize) }) and
          (results(2) must beLike { case Record(r3) => new String(r3, "UTF-8") must beEqualTo(smallRecord2) }) and
          (results(3) must beEqualTo(EndOfRecords))
      }
    }
  }

  def continueToReturnEndOfRecords = {
    val record1 = "test record data 1"
    val record2 = "test record data 2"
    val records = List(record1, record2).map(_.getBytes("UTF-8"))
    testBothCompressionTypes { compressionType =>
      val compressed = createCompressedStream(records, compressionType)
      val factory    = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike { case FactorySuccess(decompressor, _) =>
        val results = List(
          decompressor.getNextRecord,
          decompressor.getNextRecord,
          decompressor.getNextRecord,
          decompressor.getNextRecord,
          decompressor.getNextRecord
        )
        results must beLike { case List(Record(r1), Record(r3), EndOfRecords, EndOfRecords, EndOfRecords) =>
          (new String(r1, "UTF-8") must beEqualTo(record1)) and
            (new String(r3, "UTF-8") must beEqualTo(record2))
        }
      }
    }
  }

  def ensureUnsupportedVersionsRejected =
    testBothCompressionTypes { compressionType =>
      val compressed = createCompressedStreamWithVersions(
        List("test".getBytes("UTF-8")),
        compressionType,
        compressionVersion = 2, // Unsupported compression format version
        payloadVersion     = 1
      )
      val factory = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike { case UnsupportedVersionsInHeader(v1, v2) =>
        (v1 must beEqualTo(2)) and (v2 must beEqualTo(1))
      }
    }

  def ensurePayloadVersionPassedThrough =
    testBothCompressionTypes { compressionType =>
      val compressed = createCompressedStreamWithVersions(
        List("test".getBytes("UTF-8")),
        compressionType,
        compressionVersion = 1, // Valid compression format version
        payloadVersion     = 42 // Non-standard format version
      )
      val factory = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike { case FactorySuccess(decompressor, payloadVersion) =>
        (payloadVersion must beEqualTo(42)) and
          (decompressor.getNextRecord must beLike { case Record(data) =>
            new String(data, "UTF-8") must beEqualTo("test")
          })
      }
    }

  def ensureIncompleteSize =
    testBothCompressionTypes { compressionType =>
      val compressed = createIncompleteCompressedStream(compressionType, incompleteSize = true)
      val factory    = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike { case FactorySuccess(decompressor, _) =>
        decompressor.getNextRecord must beEqualTo(CorruptInput)
      }
    }

  def ensureIncompleteRecord =
    testBothCompressionTypes { compressionType =>
      val compressed = createIncompleteCompressedStream(compressionType, incompleteSize = false)
      val factory    = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike { case FactorySuccess(decompressor, _) =>
        decompressor.getNextRecord must beEqualTo(CorruptInput)
      }
    }

  def ensureEmptyStreamHandled =
    testBothCompressionTypes { compressionType =>
      val compressed = createCompressedStream(List.empty[Array[Byte]], compressionType)
      val factory    = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike { case FactorySuccess(decompressor, _) =>
        decompressor.getNextRecord must beEqualTo(EndOfRecords)
      }
    }

  def ensureZeroLengthRecordSupported = {
    val records = List(new Array[Byte](0)) // Zero-length record
    testBothCompressionTypes { compressionType =>
      val compressed = createCompressedStream(records, compressionType)
      val factory    = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike { case FactorySuccess(decompressor, _) =>
        decompressor.getNextRecord must beLike { case Record(data) =>
          data.length must beEqualTo(0)
        }
      }
    }
  }

  def ensureLargeVolumeProcessing = {
    val records = (1 to 100).map(i => s"record$i".getBytes("UTF-8")).toList
    testBothCompressionTypes { compressionType =>
      validateRecordsInOrder(records, compressionType)
    }
  }

  def ensureMixedSizesSupported = {
    val records = List("tiny", "medium " * 10, "large " * 50, "x").map(_.getBytes("UTF-8"))
    testBothCompressionTypes { compressionType =>
      validateRecordsInOrder(records, compressionType)
    }
  }

  def ensureCorruptedZstdWithTrailingNull = {
    val compressed = createCorruptedZstdWithTrailingNull()
    val factory    = new Decompressor.Zstd(1000)
    factory.build(compressed) must beLike { case FactorySuccess(decompressor, _) =>
      decompressor.getNextRecord must beEqualTo(CorruptInput)
    }
  }

  def ensureGzipZeroCrcCorrupt = {
    val compressed = createGzipWithZeroCrc()
    val factory    = new Decompressor.Gzip(1000)
    factory.build(compressed) must beLike { case FactorySuccess(decompressor, pv) =>
      val record = decompressor.getNextRecord
      decompressor.close()
      (pv must beEqualTo(TestPayloadVersion)) and
        (record must beEqualTo(CorruptInput))
    }
  }

  def ensureZstdZeroRecords = {
    val compressed = createCompressedStream(Nil, CompressionType.ZSTD)
    val factory    = new Decompressor.Zstd(1000)
    factory.build(compressed) must beLike { case FactorySuccess(decompressor, _) =>
      decompressor.getNextRecord must beEqualTo(EndOfRecords)
    }
  }

  private def validateRecordsInOrder(
    records: List[Array[Byte]],
    compressionType: CompressionType,
    maxBytesSinglePayload: Int = 1000
  ): MatchResult[FactoryResult] = {
    val compressed = createCompressedStream(records, compressionType)
    val factory    = getFactory(compressionType, maxBytesSinglePayload)
    factory.build(compressed) must beLike { case FactorySuccess(decompressor, _) =>
      val results                    = (1 to records.length + 1).map(_ => decompressor.getNextRecord).toList
      val (recordResults, endResult) = results.splitAt(records.length)

      (recordResults.zip(records).forall {
        case (Record(actual), expected) => actual.sameElements(expected)
        case _                          => false
      } must beTrue) and
        (endResult must beEqualTo(List(EndOfRecords)))
    }
  }

  private def testBothCompressionTypes[T](testFn: CompressionType => MatchResult[T]): MatchResult[T] =
    testFn(CompressionType.GZIP) and testFn(CompressionType.ZSTD)

  private def getFactory(compressionType: CompressionType, maxBytesSinglePayload: Int): Decompressor.Factory =
    compressionType match {
      case CompressionType.GZIP => new Decompressor.Gzip(maxBytesSinglePayload)
      case CompressionType.ZSTD => new Decompressor.Zstd(maxBytesSinglePayload)
    }
}
