/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams.compression

import cats.data.NonEmptyList

import com.github.luben.zstd.ZstdOutputStream

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure => BadRowFailure, Payload => BadRowPayload, Processor => BadRowProcessor}

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.zip.GZIPOutputStream

import scala.annotation.tailrec

object CompressionTestUtils {

  val TestPayloadVersion = 42

  sealed trait CompressionType
  object CompressionType {
    case object GZIP extends CompressionType
    case object ZSTD extends CompressionType
  }

  def createCompressedStream(
    records: List[Array[Byte]],
    compressionType: CompressionType,
    compressionVersion: Byte = 1,
    payloadVersion: Byte     = 1
  ): ByteBuffer =
    createCompressedStreamWithVersions(records, compressionType, compressionVersion, payloadVersion)

  def createCompressedStreamWithVersions(
    records: List[Array[Byte]],
    compressionType: CompressionType,
    compressionVersion: Byte,
    payloadVersion: Byte
  ): ByteBuffer = {
    val baos = new ByteArrayOutputStream()
    val compressionStream: OutputStream = compressionType match {
      case CompressionType.GZIP => new GZIPOutputStream(baos)
      case CompressionType.ZSTD => new ZstdOutputStream(baos)
    }

    // Write header
    compressionStream.write(compressionVersion.toInt)
    compressionStream.write(payloadVersion.toInt)

    // Write records
    records.foreach { record =>
      val sizeBytes = ByteBuffer.allocate(4)
      sizeBytes.order(ByteOrder.BIG_ENDIAN)
      sizeBytes.putInt(record.length)
      compressionStream.write(sizeBytes.array())
      compressionStream.write(record)
    }

    compressionStream.close()
    ByteBuffer.wrap(baos.toByteArray)
  }

  def createIncompleteCompressedStream(
    compressionType: CompressionType,
    incompleteSize: Boolean
  ): ByteBuffer = {
    val baos = new ByteArrayOutputStream()
    val compressionStream: OutputStream = compressionType match {
      case CompressionType.GZIP => new GZIPOutputStream(baos)
      case CompressionType.ZSTD => new ZstdOutputStream(baos)
    }

    // Write header
    compressionStream.write(1) // compression format version
    compressionStream.write(1) // payload format version

    if (incompleteSize)
      // Write incomplete size (only 2 bytes instead of 4)
      compressionStream.write(Array[Byte](0, 1))
    else {
      // Write complete size but incomplete data
      val sizeBytes = ByteBuffer.allocate(4)
      sizeBytes.order(ByteOrder.BIG_ENDIAN)
      sizeBytes.putInt(10) // Claim 10 bytes
      compressionStream.write(sizeBytes.array())
      compressionStream.write(Array[Byte](1, 2, 3)) // Only write 3 bytes
    }

    compressionStream.close()
    ByteBuffer.wrap(baos.toByteArray)
  }

  /**
   * Creates a zstd stream that will cause ZstdIOException during record reading.
   */
  def createCorruptedZstdWithTrailingNull(): ByteBuffer = {
    // Create a minimal valid zstd stream with proper Snowplow format header
    val baos              = new ByteArrayOutputStream()
    val compressionStream = new ZstdOutputStream(baos)

    // Write valid Snowplow header per the spec
    compressionStream.write(1) // compression format version
    compressionStream.write(1) // payload format version
    compressionStream.close()

    val validBytes = baos.toByteArray

    // Add corrupted data that will cause ZstdIOException during record parsing
    val corruptedData = Array[Byte](0, 0, 0, 5) ++ // Fake record size (5 bytes)
      Array.fill[Byte](20)(0) // Null bytes that cause ZstdIOException

    ByteBuffer.wrap(validBytes ++ corruptedData)
  }

  /**
   * Creates a gzip stream with the Snowplow header but zero records and a zeroed CRC32. This
   * matches what GzipCompressor currently produces when no records are added, because
   * CommittableCRC32 is never committed during initialize().
   */
  def createGzipWithZeroCrc(): ByteBuffer = {
    val baos              = new ByteArrayOutputStream()
    val compressionStream = new GZIPOutputStream(baos)

    compressionStream.write(1)
    compressionStream.write(TestPayloadVersion)
    compressionStream.close()

    val bytes = baos.toByteArray
    // Zero out the CRC32 field (first 4 bytes of the last 8-byte gzip trailer)
    val crcOffset = bytes.length - 8
    bytes(crcOffset)     = 0
    bytes(crcOffset + 1) = 0
    bytes(crcOffset + 2) = 0
    bytes(crcOffset + 3) = 0

    ByteBuffer.wrap(bytes)
  }

  def verifyFormat(
    compressed: ByteBuffer,
    expectedRecords: List[Array[Byte]],
    factory: Decompressor.Factory
  ): Boolean =
    factory.build(compressed) match {
      case Decompressor.FactorySuccess(decompressor, payloadVersion) =>
        val result = payloadVersion == TestPayloadVersion && extractAll(decompressor, expectedRecords)
        decompressor.close()
        result
      case _ =>
        false
    }

  def extractString(buffer: ByteBuffer): String =
    new String(buffer.array(), buffer.position(), buffer.remaining(), StandardCharsets.UTF_8)

  /**
   * A sentinel `toBadRow` used in tests that don't exercise the decompression-failure path but need
   * to satisfy `PayloadProvider.pipe`'s signature. Returns a distinctive `BadRow.GenericError` so
   * an unexpected invocation is easy to spot in test failures.
   */
  def sentinelDecompressionBadRow(processor: BadRowProcessor): Decompression.DecompressionError => BadRow =
    _ =>
      BadRow.GenericError(
        processor,
        BadRowFailure.GenericFailure(Instant.EPOCH, NonEmptyList.of("sentinel decompression toBadRow")),
        BadRowPayload.RawPayload("sentinel")
      )

  @tailrec
  private def extractAll(decompressor: Decompressor, remaining: List[Array[Byte]]): Boolean =
    (decompressor.getNextRecord, remaining) match {
      case (Decompressor.EndOfRecords, Nil) => true
      case (Decompressor.Record(data), head :: tail) =>
        data.sameElements(head) && extractAll(decompressor, tail)
      case _ => false
    }
}
