/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams.compression

import cats.implicits._
import com.github.luben.zstd.ZstdInputStreamNoFinalizer

import java.io.{IOException, InputStream}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.zip.GZIPInputStream

import scala.annotation.tailrec

/**
 * Responsible for decompressing and de-batching incoming stream messages
 *
 * @param maxBytesSinglePayload
 *   Each individual payload should not exceed this size after decompression. This is needed to
 *   protect app's memory.
 * @param stream
 *   Provides the decompressed bytes as a stream. The stream comprises many payloads separated via
 *   Snowplow's batching protocol. This class is responsible for identifying where each payload
 *   starts and ends.
 */
abstract class Decompressor private (maxBytesSinglePayload: Int, stream: InputStream) {
  import Decompressor._

  /**
   * Close this Decompressor and release associated resources.
   *
   * The code calling this class is expected to explicitly close this resource.
   */
  def close(): Unit

  /**
   * Decompress the next record from the input batch
   *
   * @return
   *   Hopefully the next decompressed record, or the end of records. Might return a `RecordTooBig`
   *   or `CorruptInput` if something unexpected went wrong.
   */
  def getNextRecord: GetRecordResult = {
    val either = for {
      nextSize <- readNextSize
      _ <- if (nextSize > maxBytesSinglePayload) {
             stream.skip(nextSize.toLong)
             Left(RecordTooBig(nextSize))
           } else Right(())
      bytes <- readBytes(nextSize)
    } yield Record(bytes)

    either.merge
  }

  /**
   * Reads a 32-bit integer, which has been encoded as 4 bytes in the incoming byte stream
   */
  private def readNextSize: Either[GetRecordResult, Int] =
    try {
      val bb = ByteBuffer.allocate(4)
      bb.order(ByteOrder.BIG_ENDIAN)
      streamRead(bb.array, 0, 4) match {
        case -1 => Left(EndOfRecords)
        case 4  => Right(bb.getInt)
        case _  => Left(CorruptInput)
      }
    } catch {
      case _: IOException =>
        Left(CorruptInput)
    }

  private def readBytes(nextSize: Int): Either[CorruptInput.type, Array[Byte]] =
    try {
      val arr     = new Array[Byte](nextSize)
      val numRead = streamRead(arr, 0, nextSize)
      if (numRead == nextSize) Right(arr) else Left(CorruptInput)
    } catch {
      case _: IOException =>
        Left(CorruptInput)
    }

  // Handles partial reads by recursively reading until all bytes are consumed.
  // InputStream.read() can return fewer bytes than requested even when not at EOF,
  // especially with compression streams like GZIPInputStream.
  @tailrec
  private def streamRead(
    arr: Array[Byte],
    offset: Int,
    len: Int
  ): Int =
    if (offset >= len)
      len
    else
      stream.read(arr, offset, len - offset) match {
        case -1 if offset == 0 => -1 // End of stream and no bytes read before
        case -1 if offset != 0 => offset // End of stream but some bytes read before
        case numRead           => streamRead(arr, offset + numRead, len)
      }

}

object Decompressor {

  /** The result of calling `getNextRecord` on the `Decompressor` */
  sealed trait GetRecordResult
  case class Record(value: Array[Byte]) extends GetRecordResult
  case object EndOfRecords extends GetRecordResult
  case class RecordTooBig(size: Int) extends GetRecordResult
  case object CorruptInput extends GetRecordResult

  /** An implementation of `InputStream` that streams bytes from a `ByteBuffer` */
  private class ByteBufferInputStream(bb: ByteBuffer) extends InputStream {
    override def read(): Int =
      if (bb.hasRemaining())
        bb.get() & 0xff
      else -1

    override def read(
      bytes: Array[Byte],
      off: Int,
      len: Int
    ): Int =
      if (bb.hasRemaining()) {
        val ret = Math.min(len, bb.remaining());
        bb.get(bytes, off, ret);
        ret
      } else -1
  }

  /** The result of trying to open a new `Decompressor` from a received stream message */
  sealed trait FactoryResult

  /**
   * @param decompressor
   *   The successfully opened decompressor
   * @param payloadVersion
   *   The payload format version byte read from the compressed header (second byte). Applications
   *   can use this to decide how to interpret the decompressed payloads.
   */
  case class FactorySuccess(decompressor: Decompressor, payloadVersion: Int) extends FactoryResult

  /**
   * The result of trying to open a `Decompressor` when the decompressed header contains an
   * unsupported compression format version
   *
   * Refer to the Snowplow specification for compressed messages.
   *
   * Briefly,
   *   - The first byte describes the compression format version. Currently we only recognize
   *     version `1`.
   *   - The second byte describes the application-specific payload format version.
   */
  case class UnsupportedVersionsInHeader(v1: Int, v2: Int) extends FactoryResult

  /** Factory class for opening new `Decompressor`s from an incoming `ByteBuffer` */
  abstract class Factory private[Decompressor] (maxBytesSinglePayload: Int) {

    /**
     * Stable human-readable identifier for this compression format, embedded in bad rows produced
     * during decompression.
     */
    def format: String

    // The specific type of decompressing `InputStream` managed by this factory
    protected type DStream <: InputStream

    // Open a new decompressing stream. Implemented by the concrete class
    protected def decompressorStream(compressed: InputStream): DStream

    // Close the specific type decompressing stream, to release resources
    protected def closeStream(dStream: DStream): Unit

    def build(input: ByteBuffer): FactoryResult = {
      val stream = decompressorStream(new ByteBufferInputStream(input))
      val v1     = stream.read()
      val v2     = stream.read()
      if (v1 === 1) {
        val decompressor = new Decompressor(maxBytesSinglePayload, stream) {
          override def close(): Unit = closeStream(stream)
        }
        FactorySuccess(decompressor, payloadVersion = v2)
      } else {
        closeStream(stream)
        // The Snowplow specification tells us to stop decompression if we don't recognize the compression format version
        UnsupportedVersionsInHeader(v1, v2)
      }
    }

  }

  /** Opens a `Decompressor` which decompresses received bytes using zstd decompression */
  class Zstd(maxBytesSinglePayload: Int) extends Factory(maxBytesSinglePayload) {
    override val format: String = "zstd"
    override type DStream = ZstdInputStreamNoFinalizer
    override protected def decompressorStream(compressed: InputStream): DStream =
      new ZstdInputStreamNoFinalizer(compressed)
    override protected def closeStream(dStream: DStream): Unit =
      dStream.close()
  }

  /** Opens a `Decompressor` which decompresses received bytes using gzip decompression */
  class Gzip(maxBytesSinglePayload: Int) extends Factory(maxBytesSinglePayload) {
    override val format: String = "gzip"
    override type DStream = GZIPInputStream
    override protected def decompressorStream(compressed: InputStream): DStream =
      new GZIPInputStream(compressed)
    override protected def closeStream(dStream: DStream): Unit =
      dStream.close()
  }

}
