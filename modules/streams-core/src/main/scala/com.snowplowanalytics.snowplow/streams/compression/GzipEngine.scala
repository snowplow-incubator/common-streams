/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams.compression

import java.util.zip.{CRC32, Deflater, GZIPOutputStream}

/**
 * A gzip engine. Like the zstd engine, one instance is reused across many batches — but unlike zstd
 * there is no heavy native state worth carrying over, so `begin` simply creates a fresh
 * GZIPOutputStream over the current sink and nothing is retained between batches.
 */
private[compression] class GzipEngine(compressionLevel: Int) extends Compressor.Engine {
  import GzipEngine._

  private var gzos: SizeCautiousGZIPOutputStream = _

  override def begin(sink: RewindableOutputStream): Unit =
    gzos = new SizeCautiousGZIPOutputStream(sink, compressionLevel)

  override def write(
    bytes: Array[Byte],
    off: Int,
    len: Int
  ): Unit = gzos.write(bytes, off, len)
  override def flush(): Unit       = gzos.flush()
  override def finish(): Unit      = gzos.close()
  override def footerOverhead: Int = 10

  override def mark(): Unit         = gzos.mark()
  override def rewindToMark(): Unit = gzos.rewindToMark()
  override def commit(): Unit       = gzos.commit()
}

object GzipEngine {

  /** A variation of a GZIPOutputStream which can rewind to an earlier state */
  private class SizeCautiousGZIPOutputStream(outputStream: java.io.OutputStream, compressionLevel: Int)
      extends GZIPOutputStream(outputStream, true) {

    crc   = new CommittableCRC32
    `def` = new RewindableDeflater(compressionLevel)

    def mark(): Unit         = `def`.asInstanceOf[RewindableDeflater].mark()
    def rewindToMark(): Unit = `def`.asInstanceOf[RewindableDeflater].rewindToMark()
    def commit(): Unit       = crc.asInstanceOf[CommittableCRC32].commit()
  }

  /**
   * A variation of a Deflater where we override `getTotalIn` to "lie" about the number of input
   * bytes it deflated.
   *
   * A Deflater is used internally by a GZIPOutputStream. The gzipped output concludes with an
   * integer count of the number of uncompressed bytes. We need a "rewindable" deflater because our
   * GZIPOutputStream needs to rewind the streams in case we exceed the target byte count. When this
   * happens, we make our Deflater "un-count" some of the input bytes it processed.
   */
  private class RewindableDeflater(compressionLevel: Int) extends Deflater(compressionLevel, true) {
    private var markedValue    = 0 // memoizes the value of `getTotalIn()`
    private var numRewindBytes = 0 // the number of bytes this Deflater should "un-count" after a rewind

    def mark(): Unit         = markedValue = super.getTotalIn()
    def rewindToMark(): Unit = numRewindBytes += super.getTotalIn() - markedValue

    override def getTotalIn(): Int = super.getTotalIn() - numRewindBytes
  }

  /**
   * A variation of a CRC32 which does not update its internal state until someone calls `.commit()`
   *
   * A CRC32 is used internally by a `GZIPOutputStream`. The gzipped output concludes with a CRC32
   * of the uncompressed bytes. We need a "committable" CRC32, because our GZIPOutputStream needs to
   * rewind the stream in case we exceed the target byte count.
   */
  private class CommittableCRC32 extends CRC32 {
    private case class PendingUpdate(
      b: Array[Byte],
      off: Int,
      len: Int
    )
    private var pendingUpdates: Vector[PendingUpdate] = Vector.empty

    override def update(
      b: Array[Byte],
      off: Int,
      len: Int
    ): Unit =
      pendingUpdates = pendingUpdates :+ PendingUpdate(b, off, len)

    def commit(): Unit = {
      pendingUpdates.foreach { case PendingUpdate(b, off, len) => super.update(b, off, len) }
      pendingUpdates = Vector.empty
    }
  }
}
