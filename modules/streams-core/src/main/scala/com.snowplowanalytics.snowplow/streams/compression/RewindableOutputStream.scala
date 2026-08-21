/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams.compression

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

/**
 * A variation of ByteArrayOutputStream which can rewind to an earlier state
 *
 * This output stream receives and stores the compressed bytes produced by the compressor. It needs
 * to be "rewindable" to an earlier state in case we exceed the target byte count.
 *
 * This private class is used internally by various implementations of `Compressor`
 */
private[compression] class RewindableOutputStream(targetSize: Int) extends ByteArrayOutputStream(targetSize) {

  // This local state is not thread-safe. Make the external code responsible for sequencing access to the compressors.
  private var marker: Int = 0 // memoizes the value of `count`

  /** Returns the result of this ByteArrayOutputStream wrapped as a ByteBuffer */
  def toByteBuffer(): ByteBuffer =
    ByteBuffer.wrap(buf, 0, count)

  // Drop a marker. We might need to rewind _this_ state.
  def mark(): Unit =
    marker = count

  // Rewind to last memoized state when `mark()` got called
  def rewindToMark(): Unit =
    count = marker

  /**
   * Append the remaining bytes of a (possibly direct) ByteBuffer, draining it. This is our own
   * helper (used by the zstd engine to drain its direct staging buffer), not part of the
   * OutputStream contract. Once we are over target we skip the copy because these bytes are
   * destined to be rewound, but we still advance `count` so the caller's size check fires. `buf` is
   * pre-sized to `targetSize` and committed output never exceeds it, so no growth is ever needed
   * here.
   */
  private[compression] def append(src: ByteBuffer): Unit = {
    val len = src.remaining()
    if (count + len > targetSize) {
      count += len
      val _ = src.position(src.limit())
    } else {
      src.get(buf, count, len)
      count += len
    }
  }

  override def write(
    b: Array[Byte],
    off: Int,
    len: Int
  ): Unit =
    if (count + len > targetSize) {
      // No point in writing, because we are sure to do a rewind.
      // Skip writing, so we avoid doing an expensive grow of buf.
      count += len
    } else {
      super.write(b, off, len)
    }

}
