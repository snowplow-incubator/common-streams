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
import cats.effect.testing.specs2.CatsEffect
import org.specs2.Specification
import org.specs2.matcher.MatchResult

class CompressorLifecycleSpec extends Specification with CatsEffect {
  import CompressionTestUtils.{TestPayloadVersion, verifyFormat}

  def is = s2"""
  The Compressor lifecycle guard should
    throw if addRecord is called before reset (uninitialised)              $addRecordBeforeReset
    throw if result is called before reset (uninitialised)                 $resultBeforeReset
    throw if addRecord is called after result                     $addRecordAfterResult
    throw if addRecord is called after a rejected record          $addRecordAfterRejection
    throw if result is called twice on the same batch             $resultTwice
    throw if result is called after the first record is rejected  $resultAfterFirstRejection
    still allow reuse: reset then addRecord after taking result   $reuseAfterResult
    allow reset straight after the first record is rejected       $resetAfterFirstRejection
    discard an unfinished batch with accepted records on reset    $resetPartwayThroughBatch
  """

  private val record = "hello".getBytes("UTF-8")

  // Incompressible, so it is reliably rejected at a small target size.
  private val bigRecord: Array[Byte] = {
    val a = new Array[Byte](5000)
    new scala.util.Random(1).nextBytes(a)
    a
  }

  private def add(c: Compressor, r: Array[Byte] = record): Boolean = c.addRecord(r, 0, r.length)

  private def discard(a: Any): Unit = {
    val _ = a
  }

  // Run the same assertions against both engines, since the guard lives in the engine-agnostic Impl.
  private def eachEngine(f: Compressor => MatchResult[Any]): IO[MatchResult[Any]] =
    for {
      g <- CompressorFactory.gzip(3).resource[IO].use(c => IO(f(c)))
      z <- CompressorFactory.zstd(3).resource[IO].use(c => IO(f(c)))
    } yield g and z

  // As eachEngine, but also hands over the matching decompressor so a test can decode the output.
  private def eachEngineWithDecoder(f: (Compressor, Decompressor.Factory) => MatchResult[Any]): IO[MatchResult[Any]] =
    for {
      g <- CompressorFactory.gzip(3).resource[IO].use(c => IO(f(c, new Decompressor.Gzip(Int.MaxValue))))
      z <- CompressorFactory.zstd(3).resource[IO].use(c => IO(f(c, new Decompressor.Zstd(Int.MaxValue))))
    } yield g and z

  def addRecordBeforeReset: IO[MatchResult[Any]] = eachEngine { c =>
    add(c) must throwAn[IllegalStateException]
  }

  def resultBeforeReset: IO[MatchResult[Any]] = eachEngine { c =>
    c.result must throwAn[IllegalStateException]
  }

  def addRecordAfterResult: IO[MatchResult[Any]] = eachEngine { c =>
    c.reset(TestPayloadVersion, 10000)
    discard(add(c))
    discard(c.result)
    add(c) must throwAn[IllegalStateException]
  }

  def addRecordAfterRejection: IO[MatchResult[Any]] = eachEngine { c =>
    c.reset(TestPayloadVersion, 100)
    val rejected = add(c, bigRecord)
    (rejected must beFalse) and (add(c) must throwAn[IllegalStateException])
  }

  def resultTwice: IO[MatchResult[Any]] = eachEngine { c =>
    c.reset(TestPayloadVersion, 10000)
    discard(add(c))
    discard(c.result)
    c.result must throwAn[IllegalStateException]
  }

  def resultAfterFirstRejection: IO[MatchResult[Any]] = eachEngine { c =>
    c.reset(TestPayloadVersion, 100)
    val rejected = add(c, bigRecord)
    (rejected must beFalse) and (c.result must throwAn[IllegalStateException])
  }

  def reuseAfterResult: IO[MatchResult[Any]] = eachEngine { c =>
    c.reset(TestPayloadVersion, 10000)
    discard(add(c))
    discard(c.result)
    c.reset(TestPayloadVersion, 10000)
    add(c) must beTrue
  }

  def resetAfterFirstRejection: IO[MatchResult[Any]] = eachEngineWithDecoder { (c, decoder) =>
    c.reset(TestPayloadVersion, 100)
    val rejected = add(c, bigRecord)
    c.reset(TestPayloadVersion, 10000)
    val accepted   = add(c)
    val compressed = c.result
    // Only a decode proves that no engine state leaked across the reset.
    (rejected must beFalse) and
      (accepted must beTrue: MatchResult[Any]) and
      (c.recordCount must beEqualTo(1): MatchResult[Any]) and
      (verifyFormat(compressed, List(record), decoder) must beTrue)
  }

  def resetPartwayThroughBatch: IO[MatchResult[Any]] = eachEngineWithDecoder { (c, decoder) =>
    val discarded = "discarded".getBytes("UTF-8")
    c.reset(TestPayloadVersion, 10000)
    discard(add(c, discarded))
    discard(add(c, discarded))
    c.reset(TestPayloadVersion, 10000)
    val accepted   = add(c)
    val compressed = c.result
    (accepted must beTrue) and
      (c.recordCount must beEqualTo(1): MatchResult[Any]) and
      (verifyFormat(compressed, List(record), decoder) must beTrue)
  }
}
