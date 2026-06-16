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
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification

class GzipCompressorSpec extends Specification with CatsEffect {
  import CompressionTestUtils._

  private val factory = new Decompressor.Gzip(Int.MaxValue)

  override def is = s2"""
  GzipCompressor should:
    handle single record                                      $g1
    handle multiple records                                   $g2
    reject record that would exceed target size               $g3
    handle non-zero offset in addRecord                       $g4
    handle empty record                                       $g5
    produce correct format headers                            $g6
    handle boundary case at exact target size                 $g7
    reject record that exceeds target size by one byte        $g8
    handle large records                                      $g9
    produce expected bytes when no records added              $g10
    produce invalid output when first record is rejected      $g11
  """

  def g1 = {
    val compressor = GzipCompressor.factory(6).buildAndInitialize(1000, TestPayloadVersion)
    val record     = "test-record-g1".getBytes("UTF-8")

    val addResult  = compressor.addRecord(record, 0, record.length)
    val compressed = compressor.result

    (addResult must beTrue) and
      (verifyFormat(compressed, List(record), factory) must beTrue)
  }

  def g2 = {
    val compressor = GzipCompressor.factory(6).buildAndInitialize(1000, TestPayloadVersion)
    val record1    = "record1".getBytes("UTF-8")
    val record2    = "record2".getBytes("UTF-8")
    val record3    = "record3".getBytes("UTF-8")

    val r1 = compressor.addRecord(record1, 0, record1.length)
    val r2 = compressor.addRecord(record2, 0, record2.length)
    val r3 = compressor.addRecord(record3, 0, record3.length)

    val compressed = compressor.result

    (r1 must beTrue) and
      (r2 must beTrue) and
      (r3 must beTrue) and
      (verifyFormat(compressed, List(record1, record2, record3), factory) must beTrue)
  }

  def g3 = {
    val compressor  = GzipCompressor.factory(6).buildAndInitialize(30, TestPayloadVersion)
    val largeRecord = ("large-record" + "x" * 1000).getBytes("UTF-8")

    compressor.addRecord(largeRecord, 0, largeRecord.length) must beFalse
  }

  def g4 = {
    val compressor     = GzipCompressor.factory(6).buildAndInitialize(1000, TestPayloadVersion)
    val fullData       = "prefix_test_record_suffix".getBytes("UTF-8")
    val expectedRecord = "test_record".getBytes("UTF-8")

    val addResult  = compressor.addRecord(fullData, 7, 11) // Extract "test_record"
    val compressed = compressor.result

    (addResult must beTrue) and
      (verifyFormat(compressed, List(expectedRecord), factory) must beTrue)
  }

  def g5 = {
    val compressor  = GzipCompressor.factory(6).buildAndInitialize(1000, TestPayloadVersion)
    val emptyRecord = Array.empty[Byte]

    val addResult  = compressor.addRecord(emptyRecord, 0, 0)
    val compressed = compressor.result

    (addResult must beTrue) and
      (verifyFormat(compressed, List(emptyRecord), factory) must beTrue)
  }

  def g6 = {
    val compressor = GzipCompressor.factory(6).buildAndInitialize(1000, TestPayloadVersion)
    val record     = "header-test".getBytes("UTF-8")
    val _          = compressor.addRecord(record, 0, record.length)
    val compressed = compressor.result

    factory.build(compressed) must beLike { case Decompressor.FactorySuccess(decompressor, payloadVersion) =>
      decompressor.close()
      payloadVersion must_== TestPayloadVersion // Payload format version
    }
  }

  def g7 = {
    // Test boundary case: use a record that compresses to exactly the target size
    val compressor = GzipCompressor.factory(6).buildAndInitialize(35, TestPayloadVersion)

    // The string "abc" (3 bytes) compresses to exactly 35 bytes total:
    // - 2 bytes: compression format headers (1 + 1)
    // - 4 bytes: record length as big-endian int32 (payload size)
    // - ~29 bytes: gzip overhead + compressed "abc" content, where gzip overhead includes:
    //   * 10 bytes: gzip header (magic number, compression method, flags, timestamp, etc.)
    //   * ~15 bytes: deflate compressed data for "abc" (includes huffman coding, LZ77 references)
    //   * 8 bytes: gzip trailer (CRC32 checksum + uncompressed size)
    //   * Small data like "abc" has poor compression ratio due to fixed overhead
    // Total = 2 + 4 + 29 = 35 bytes exactly
    val exactFitRecord = "abc".getBytes("UTF-8")

    val addResult      = compressor.addRecord(exactFitRecord, 0, exactFitRecord.length)
    val compressed     = compressor.result
    val compressedSize = compressed.remaining()

    (addResult must beTrue) and
      (verifyFormat(compressed, List(exactFitRecord), factory) must beTrue) and
      // Verify we hit exactly the target size (boundary case)
      (compressedSize must_== 35)
  }

  def g8 = {
    // Test one-over boundary case: use a record that compresses to one byte over target size
    val compressor = GzipCompressor.factory(6).buildAndInitialize(35, TestPayloadVersion)

    // The string "abcd" (4 bytes) compresses to exactly 36 bytes total (one over target):
    val overTargetRecord = "abcd".getBytes("UTF-8")

    compressor.addRecord(overTargetRecord, 0, overTargetRecord.length) must beFalse // Should be rejected as it exceeds target size
  }

  def g9 = {
    val compressor = GzipCompressor.factory(6).buildAndInitialize(50000, TestPayloadVersion)

    // Create a genuinely large record (5KB of data)
    val record = ("x" * 5000).getBytes("UTF-8")

    val addResult      = compressor.addRecord(record, 0, record.length)
    val compressed     = compressor.result
    val compressedSize = compressed.remaining()

    (addResult must beTrue) and
      (verifyFormat(compressed, List(record), factory) must beTrue) and
      // Verify compression actually worked (compressed should be much smaller than original)
      ((compressedSize < record.length) must beTrue)
  }

  def g10 = {
    val compressor = GzipCompressor.factory(6).buildAndInitialize(1000, TestPayloadVersion)
    val compressed = compressor.result

    val bytes = new Array[Byte](compressed.remaining())
    compressed.get(bytes)

    // Known issue: CRC32 is zeroed because CommittableCRC32 is never committed
    // during initialize(). See PDP-2613.
    // Gzip header (10 bytes)              — static, written by GZIPOutputStream constructor
    // Deflate of header [1, 42] (4 bytes) — Snowplow header compressed by deflater
    // CRC32 (4 bytes)                     — zeroed (PDP-2613)
    // Uncompressed size (4 bytes)         — 2 bytes of Snowplow header [1, 42]
    // Note: byte 9 (OS field) is platform-dependent: 0xFF on macOS, 0x00 on Linux
    val osByte: Byte = if (System.getProperty("os.name").toLowerCase.contains("linux")) 0 else -1
    val expected     = Array[Byte](31, -117, 8, 0, 0, 0, 0, 0, 0, osByte, 99, -44, 2, 0, 0, 0, 0, 0, 2, 0, 0, 0)

    (compressor.recordCount must beEqualTo(0): MatchResult[Any]) and
      (bytes must beEqualTo(expected))
  }

  def g11 = {
    val compressor  = GzipCompressor.factory(6).buildAndInitialize(10, TestPayloadVersion)
    val largeRecord = new Array[Byte](10000)

    val isAdded = compressor.addRecord(largeRecord, 0, largeRecord.length)

    (isAdded must beFalse: MatchResult[Any]) and
      (compressor.result must throwAn[IndexOutOfBoundsException]("null"))
  }
}
