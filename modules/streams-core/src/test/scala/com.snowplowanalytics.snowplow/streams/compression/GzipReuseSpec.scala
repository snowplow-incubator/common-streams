/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.streams.compression

import cats.effect.IO
import cats.implicits._
import cats.effect.testing.specs2.CatsEffect
import org.specs2.Specification
import org.specs2.matcher.MatchResult

class GzipReuseSpec extends Specification with CatsEffect {
  import CompressionTestUtils.{TestPayloadVersion, verifyFormat}

  def is = s2"""
  A reused GzipEngine-backed Compressor should
    give the same output as a fresh compressor after rejected and abandoned batches  $sameOutputAsFresh
    fail if used after its Resource is released                                      $failsAfterRelease
  """

  private val df = new Decompressor.Gzip(Int.MaxValue)

  private val records = List("alpha", "beta", "gamma", "delta").map(_.getBytes("UTF-8"))

  // Random bytes so the record does not compress below the target size and gets rejected.
  private val big: Array[Byte] = {
    val a = new Array[Byte](10000)
    new scala.util.Random(99).nextBytes(a)
    a
  }

  private def add(c: Compressor, r: Array[Byte]): Boolean = c.addRecord(r, 0, r.length)

  private def compress(c: Compressor): Array[Byte] = {
    c.reset(TestPayloadVersion, 200)
    records.foreach(r => add(c, r))
    val bb    = c.result
    val bytes = new Array[Byte](bb.remaining())
    bb.get(bytes)
    bytes
  }

  def sameOutputAsFresh: IO[MatchResult[Any]] = {
    val fresh: IO[Array[Byte]] =
      CompressorFactory.gzip(6).resource[IO].use(c => IO(compress(c)))

    val reused: IO[(Boolean, Boolean, Array[Byte])] =
      CompressorFactory.gzip(6).resource[IO].use { c =>
        IO {
          // A finished batch whose last record was rejected
          c.reset(TestPayloadVersion, 200)
          val r1Added  = add(c, "r1".getBytes("UTF-8"))
          val rejected = add(c, big)
          val _        = c.result

          // A batch whose first record was rejected, abandoned without `result`
          c.reset(TestPayloadVersion, 200)
          val firstRejected = add(c, big)

          // A batch with accepted records, abandoned without `result`
          c.reset(TestPayloadVersion, 200)
          records.foreach(r => add(c, r))

          (r1Added && !firstRejected, rejected, compress(c))
        }
      }

    (reused, fresh).mapN { case ((ok, rejected, afterReuse), freshBytes) =>
      (ok must beTrue: MatchResult[Any]) and
        (rejected must beFalse) and
        (afterReuse.toList must beEqualTo(freshBytes.toList)) and
        (verifyFormat(java.nio.ByteBuffer.wrap(afterReuse), records, df) must beTrue)
    }
  }

  def failsAfterRelease: IO[MatchResult[Any]] =
    CompressorFactory
      .gzip(6)
      .resource[IO]
      .use(c => IO.pure(c))
      .map { c =>
        c.reset(TestPayloadVersion, 200) must throwA[Throwable]
      }
}
