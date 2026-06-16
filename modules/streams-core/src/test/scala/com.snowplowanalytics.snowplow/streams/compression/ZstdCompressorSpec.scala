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

class ZstdCompressorSpec extends Specification with CatsEffect {
  import CompressionTestUtils._

  private val factory = new Decompressor.Zstd(Int.MaxValue)

  override def is = s2"""
  ZstdCompressor should:
    handle single record                                      $z1
    handle multiple records                                   $z2
    reject record that would exceed target size               $z3
    handle non-zero offset in addRecord                       $z4
    handle empty record                                       $z5
    produce correct format headers                            $z6
    handle boundary case at exact target size                 $z7
    reject record that exceeds target size by one byte        $z8
    handle large records                                      $z9
    produce expected bytes when no records added              $z10
    produce invalid stream when first record is rejected      $z11
  """

  def z1 = {
    val compressor = ZstdCompressor.factory(3).buildAndInitialize(1000, TestPayloadVersion)
    val record     = "test-record-z1".getBytes("UTF-8")

    val addResult  = compressor.addRecord(record, 0, record.length)
    val compressed = compressor.result

    (addResult must beTrue) and
      (verifyFormat(compressed, List(record), factory) must beTrue)
  }

  def z2 = {
    val compressor = ZstdCompressor.factory(3).buildAndInitialize(1000, TestPayloadVersion)
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

  def z3 = {
    val compressor  = ZstdCompressor.factory(3).buildAndInitialize(20, TestPayloadVersion)
    val largeRecord = ("large-record" + "x" * 1000).getBytes("UTF-8")

    compressor.addRecord(largeRecord, 0, largeRecord.length) must beFalse
  }

  def z4 = {
    val compressor     = ZstdCompressor.factory(3).buildAndInitialize(1000, TestPayloadVersion)
    val fullData       = "prefix_test_record_suffix".getBytes("UTF-8")
    val expectedRecord = "test_record".getBytes("UTF-8")

    val addResult  = compressor.addRecord(fullData, 7, 11) // Extract "test_record"
    val compressed = compressor.result

    (addResult must beTrue) and
      (verifyFormat(compressed, List(expectedRecord), factory) must beTrue)
  }

  def z5 = {
    val compressor  = ZstdCompressor.factory(3).buildAndInitialize(1000, TestPayloadVersion)
    val emptyRecord = Array.empty[Byte]

    val addResult  = compressor.addRecord(emptyRecord, 0, 0)
    val compressed = compressor.result

    (addResult must beTrue) and
      (verifyFormat(compressed, List(emptyRecord), factory) must beTrue)
  }

  def z6 = {
    val compressor = ZstdCompressor.factory(3).buildAndInitialize(1000, TestPayloadVersion)
    val record     = "header-test".getBytes("UTF-8")
    val _          = compressor.addRecord(record, 0, record.length)
    val compressed = compressor.result

    factory.build(compressed) must beLike { case Decompressor.FactorySuccess(decompressor, payloadVersion) =>
      decompressor.close()
      payloadVersion must_== TestPayloadVersion // Payload format version
    }
  }

  def z7 = {
    // Test boundary case: use a record that compresses to exactly the target size
    val compressor = ZstdCompressor.factory(3).buildAndInitialize(21, TestPayloadVersion)

    // The string "abc" (3 bytes) compresses to exactly 21 bytes total with Zstd:
    // - 2 bytes: compression format headers (1 + 1)
    // - 4 bytes: record length as big-endian int32 (payload size)
    // - ~15 bytes: zstd overhead + compressed "abc" content, where zstd overhead includes:
    //   * ~4 bytes: zstd frame header (magic number, frame descriptor)
    //   * ~7 bytes: zstd compressed data for "abc" (dictionary, literals, sequences)
    //   * ~4 bytes: optional checksum (if enabled)
    //   * Zstd is much more efficient than gzip for small data
    // Total = 2 + 4 + 15 = 21 bytes exactly
    val exactFitRecord = "abc".getBytes("UTF-8")

    val addResult      = compressor.addRecord(exactFitRecord, 0, exactFitRecord.length)
    val compressed     = compressor.result
    val compressedSize = compressed.remaining()

    (addResult must beTrue) and
      (verifyFormat(compressed, List(exactFitRecord), factory) must beTrue) and
      // Verify we hit exactly the target size (boundary case)
      (compressedSize must_== 21)
  }

  def z8 = {
    // Test one-over boundary case: use a record that compresses to one byte over target size
    val compressor = ZstdCompressor.factory(3).buildAndInitialize(21, TestPayloadVersion)

    // The string "abcd" (4 bytes) compresses to exactly 22 bytes total (one over target):
    val overTargetRecord = "abcd".getBytes("UTF-8")

    compressor.addRecord(overTargetRecord, 0, overTargetRecord.length) must beFalse // Should be rejected as it exceeds target size
  }

  def z9 = {
    val compressor = ZstdCompressor.factory(3).buildAndInitialize(50000, TestPayloadVersion)

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

  def z10 = {
    val compressor = ZstdCompressor.factory(3).buildAndInitialize(1000, TestPayloadVersion)
    val compressed = compressor.result

    val bytes = new Array[Byte](compressed.remaining())
    compressed.get(bytes)

    // 40, -75, 47, -3  — zstd magic number (0x28B52FFD)
    // 0, 88            — frame header (descriptor + window size)
    // 17, 0, 0         — block header (last=true, type=raw, size=2)
    // 1, 42            — Snowplow header uncompressed (compression version=1, payload version=42)
    val expected = Array[Byte](40, -75, 47, -3, 0, 88, 17, 0, 0, 1, 42)

    (compressor.recordCount must beEqualTo(0): MatchResult[Any]) and
      (bytes must beEqualTo(expected))
  }

  def z11 = {
    val compressor  = ZstdCompressor.factory(3).buildAndInitialize(20, TestPayloadVersion)
    val largeRecord = new Array[Byte](10000)

    val isAdded    = compressor.addRecord(largeRecord, 0, largeRecord.length)
    val compressed = compressor.result

    (isAdded must beFalse: MatchResult[Any]) and
      (factory.build(compressed) must throwA[com.github.luben.zstd.ZstdIOException]("Unknown frame descriptor"))
  }
}
