/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams.compression

import java.util.zip.{CRC32, Deflater, DeflaterOutputStream}

/**
 * A reusable gzip engine. The native Deflater (whose zlib state is several hundred KB off-heap) is
 * allocated once and reused across every batch; `begin` resets it in place, avoiding the per-batch
 * native allocation that `GZIPOutputStream` incurs, and which is only freed by a later GC.
 *
 * The deflater emits raw deflate; this engine writes the gzip header and trailer itself (RFC 1952).
 * The trailer's CRC32 and input size cover committed bytes only, so a rewound record is excluded.
 *
 * @param deflater
 *   a raw (`nowrap = true`) Deflater whose lifetime is managed by the caller (freed via Resource).
 */
private[compression] class GzipEngine(deflater: Deflater) extends Compressor.Engine {
  import GzipEngine._

  private val crc = new CRC32
  private var committedBytes: Long           = 0L
  private var pending: Vector[PendingUpdate] = Vector.empty
  private var sink: RewindableOutputStream   = _
  private var dos: DeflaterOutputStream      = _

  override def begin(s: RewindableOutputStream): Unit = {
    deflater.reset()
    crc.reset()
    committedBytes = 0L
    pending        = Vector.empty
    sink           = s
    sink.write(Header, 0, Header.length)
    // syncFlush = true, so that `flush` emits everything written so far, which the size check relies on.
    // This DeflaterOutputStream does not own the deflater, so it never calls `end()` on it.
    dos = new DeflaterOutputStream(sink, deflater, true)
  }

  override def write(
    bytes: Array[Byte],
    off: Int,
    len: Int
  ): Unit = {
    dos.write(bytes, off, len)
    pending = pending :+ PendingUpdate(bytes, off, len)
  }

  override def flush(): Unit = dos.flush()

  override def finish(): Unit = {
    dos.finish()
    writeIntLE(crc.getValue)
    writeIntLE(committedBytes)
  }

  override def footerOverhead: Int = 10

  override def rewindToMark(): Unit = pending = Vector.empty

  override def commit(): Unit = {
    pending.foreach { case PendingUpdate(b, off, len) =>
      crc.update(b, off, len)
      committedBytes += len
    }
    pending = Vector.empty
  }

  /** Writes the low 32 bits of `value`, little-endian, as the gzip trailer requires. */
  private def writeIntLE(value: Long): Unit = {
    val bytes = Array[Byte](value.toByte, (value >> 8).toByte, (value >> 16).toByte, (value >> 24).toByte)
    sink.write(bytes, 0, bytes.length)
  }
}

object GzipEngine {

  private case class PendingUpdate(
    b: Array[Byte],
    off: Int,
    len: Int
  )

  /**
   * The 10-byte gzip header: magic, deflate method, no flags, no mtime, no extra flags, OS unknown.
   * This is exactly what JDK 21's GZIPOutputStream writes.
   */
  private val Header: Array[Byte] =
    Array(0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff).map(_.toByte)
}
