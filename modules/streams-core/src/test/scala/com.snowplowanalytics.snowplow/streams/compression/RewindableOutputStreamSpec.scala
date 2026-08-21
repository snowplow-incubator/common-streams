/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams.compression

import org.specs2.mutable.Specification

import java.nio.ByteBuffer

class RewindableOutputStreamSpec extends Specification {
  override def is = s2"""
  RewindableOutputStream.append(ByteBuffer) should
    append the buffer's remaining bytes and drain it                            $test1
    skip the physical copy but still advance size, and rewind cleanly           $test2
  """

  def test1 = {
    val rwos = new RewindableOutputStream(100)
    val src  = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))
    rwos.append(src)

    val out = new Array[Byte](rwos.size())
    rwos.toByteBuffer().get(out)

    (rwos.size() must_== 4) and
      (src.remaining() must_== 0) and
      (out.toList must_== List[Byte](1, 2, 3, 4))
  }

  def test2 = {
    val rwos = new RewindableOutputStream(4)
    rwos.append(ByteBuffer.wrap(Array[Byte](1, 2, 3))) // under target, copied
    rwos.mark() // committed boundary (count == 3)
    rwos.append(ByteBuffer.wrap(Array[Byte](4, 5, 6))) // pushes over target, skipped
    val sizeWhileOver = rwos.size()
    rwos.rewindToMark() // caller rewinds after detecting over-target

    val out = new Array[Byte](rwos.size())
    rwos.toByteBuffer().get(out)

    (sizeWhileOver must_== 6) and // skip still advanced size, so the target check fires
      (rwos.size() must_== 3) and // rewind restored the committed size
      (out.toList must_== List[Byte](1, 2, 3))
  }
}
