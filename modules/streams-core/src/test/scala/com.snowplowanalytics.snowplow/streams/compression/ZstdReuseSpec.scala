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
import cats.implicits._
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import java.nio.ByteBuffer

import scala.annotation.tailrec

class ZstdReuseSpec extends Specification with CatsEffect {
  import CompressionTestUtils.TestPayloadVersion

  def is = s2"""
  A reused ZstdEngine-backed Compressor should
    produce a correct frame for every batch when reused across many resets    $reuseAcrossManyBatches
    give the same output as a freshly-created compressor for the same batch  $sameOutputAsFresh
    fully clear the ctx window after a rejected record, via reset             $resetClearsRejectedRecordWindow
  """

  private val df = new Decompressor.Zstd(Int.MaxValue)

  private val batches: List[List[Array[Byte]]] =
    List(
      List("alpha", "beta", "gamma"),
      List("delta"),
      List("epsilon", "zeta", "eta", "theta"),
      Nil,
      List("iota", "kappa")
    ).map(_.map(_.getBytes("UTF-8")))

  @tailrec
  private def drain(d: Decompressor, acc: List[Array[Byte]] = Nil): List[Array[Byte]] =
    d.getNextRecord match {
      case Decompressor.Record(bytes) => drain(d, bytes :: acc)
      case Decompressor.EndOfRecords  => d.close(); acc.reverse
      case other                      => d.close(); throw new RuntimeException(s"Unexpected: $other")
    }

  private def decode(bb: ByteBuffer): List[Array[Byte]] =
    df.build(bb) match {
      case Decompressor.FactorySuccess(d, _) => drain(d)
      case other                             => throw new RuntimeException(s"Unexpected: $other")
    }

  def reuseAcrossManyBatches: IO[MatchResult[Any]] =
    CompressorFactory
      .zstd(3)
      .resource[IO]
      .use { c =>
        IO {
          batches.map { records =>
            c.reset(TestPayloadVersion, 10000)
            records.foreach(r => c.addRecord(r, 0, r.length))
            decode(c.result)
          }
        }
      }
      .map { decodedBatches =>
        val ok = decodedBatches.zip(batches).forall { case (decoded, original) =>
          decoded.length == original.length &&
          decoded.zip(original).forall { case (a, e) => a.sameElements(e) }
        }
        ok must beTrue
      }

  def sameOutputAsFresh: IO[MatchResult[Any]] = {
    val records = List("one", "two", "three", "four").map(_.getBytes("UTF-8"))

    def compressOnce: IO[Array[Byte]] =
      CompressorFactory.zstd(3).resource[IO].use { c =>
        IO {
          c.reset(TestPayloadVersion, 10000)
          records.foreach(r => c.addRecord(r, 0, r.length))
          val bb    = c.result
          val bytes = new Array[Byte](bb.remaining())
          bb.get(bytes)
          bytes
        }
      }

    // Reuse one compressor for a throwaway batch, then the real batch; compare to a fresh one.
    val reused: IO[Array[Byte]] =
      CompressorFactory.zstd(3).resource[IO].use { c =>
        IO {
          c.reset(TestPayloadVersion, 10000)
          List("warmup-a", "warmup-b").foreach(r => c.addRecord(r.getBytes("UTF-8"), 0, r.length))
          val _ = c.result // discard
          c.reset(TestPayloadVersion, 10000)
          records.foreach(r => c.addRecord(r, 0, r.length))
          val bb    = c.result
          val bytes = new Array[Byte](bb.remaining())
          bb.get(bytes)
          bytes
        }
      }

    (compressOnce, reused).mapN { (fresh, afterReuse) =>
      afterReuse.toList must beEqualTo(fresh.toList)
    }
  }

  def resetClearsRejectedRecordWindow: IO[MatchResult[Any]] = {
    val batch2 = List("alpha", "beta", "gamma", "delta").map(_.getBytes("UTF-8"))

    val reused: IO[(Boolean, Boolean, Array[Byte])] =
      CompressorFactory.zstd(3).resource[IO].use { c =>
        IO {
          c.reset(TestPayloadVersion, 200)
          val r1      = "r1".getBytes("UTF-8")
          val r1Added = c.addRecord(r1, 0, r1.length)

          // Random bytes so the record does not compress below the target size and gets rejected.
          val big = new Array[Byte](10000)
          new scala.util.Random(99).nextBytes(big)
          val rejected = c.addRecord(big, 0, big.length)

          // This result contains only the accepted records, but leaves the rejected
          // record's data in the native ctx window until the next reset.
          val _ = c.result

          c.reset(TestPayloadVersion, 200)
          batch2.foreach(r => c.addRecord(r, 0, r.length))
          val bb    = c.result
          val bytes = new Array[Byte](bb.remaining())
          bb.get(bytes)
          (r1Added, rejected, bytes)
        }
      }

    val fresh: IO[Array[Byte]] =
      CompressorFactory.zstd(3).resource[IO].use { c =>
        IO {
          c.reset(TestPayloadVersion, 200)
          batch2.foreach(r => c.addRecord(r, 0, r.length))
          val bb    = c.result
          val bytes = new Array[Byte](bb.remaining())
          bb.get(bytes)
          bytes
        }
      }

    (reused, fresh).mapN { case ((r1Added, rejected, afterReuse), freshBytes) =>
      val decoded  = decode(ByteBuffer.wrap(afterReuse))
      val decodeOk = decoded.length == batch2.length && decoded.zip(batch2).forall { case (a, e) => a.sameElements(e) }

      (r1Added must beTrue: MatchResult[Any]) and
        (rejected must beFalse) and
        (afterReuse.toList must beEqualTo(freshBytes.toList)) and
        (decodeOk must beTrue)
    }
  }
}
