/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams.compression

import com.github.luben.zstd.{EndDirective, ZstdCompressCtx, ZstdOutputStreamNoFinalizer}

import java.nio.ByteBuffer

/**
 * A reusable zstd engine. The native ZstdCompressCtx and the direct staging buffers are allocated
 * once and reused across every batch; `begin` resets the context's session in place, avoiding the
 * per-batch native allocation that the streaming OutputStream classes incur.
 *
 * @param ctx
 *   a ZstdCompressCtx whose lifetime is managed by the caller (freed via Resource).
 */
private[compression] class ZstdEngine(ctx: ZstdCompressCtx, compressionLevel: Int) extends Compressor.Engine {

  // Small fixed buffers, reused for the engine's whole lifetime. `stagingOut` is what zstd writes
  // into (drained repeatedly into the heap sink); `stagingIn` is used to copy heap record bytes into
  // a direct buffer, in chunks, because the streaming API requires direct buffers.
  private val bufSize    = ZstdOutputStreamNoFinalizer.recommendedCOutSize().toInt
  private val stagingOut = ByteBuffer.allocateDirect(bufSize)
  private val stagingIn  = ByteBuffer.allocateDirect(bufSize)

  private var sink: RewindableOutputStream = _

  override def begin(s: RewindableOutputStream): Unit = {
    sink = s
    ctx.reset() // clears session AND parameters, but keeps the native workspace
    ctx.setLevel(compressionLevel) // re-apply parameters after reset (cheap; no reallocation)
    // Checksums OFF is correctness-critical, not a perf tweak: it makes the frame epilogue a
    // content-independent empty-last-block marker, which is what lets `result` truncate at the last
    // committed block boundary and cap the frame with a valid epilogue.
    val _ = ctx.setChecksum(false)
  }

  override def write(
    bytes: Array[Byte],
    off: Int,
    len: Int
  ): Unit = {
    var pos       = off
    var remaining = len
    while (remaining > 0) {
      val chunk = Math.min(remaining, stagingIn.capacity)
      stagingIn.clear()
      stagingIn.put(bytes, pos, chunk)
      stagingIn.flip()
      while (stagingIn.hasRemaining) {
        stagingOut.clear()
        ctx.compressDirectByteBufferStream(stagingOut, stagingIn, EndDirective.CONTINUE)
        drain()
      }
      pos += chunk
      remaining -= chunk
    }
  }

  override def flush(): Unit  = flushWith(EndDirective.FLUSH)
  override def finish(): Unit = flushWith(EndDirective.END)

  override def footerOverhead: Int = 3

  private def flushWith(directive: EndDirective): Unit = {
    stagingIn.clear()
    stagingIn.limit(0) // empty input
    var done = false
    while (!done) {
      stagingOut.clear()
      done = ctx.compressDirectByteBufferStream(stagingOut, stagingIn, directive)
      drain()
    }
  }

  private def drain(): Unit = {
    stagingOut.flip()
    sink.append(stagingOut)
  }
}
