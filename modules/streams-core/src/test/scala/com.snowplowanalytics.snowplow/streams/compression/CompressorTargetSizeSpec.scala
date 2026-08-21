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
import scala.util.Random

/**
 * Exercises the per-batch `targetSize` capability: a single long-lived Compressor may be `reset`
 * with a different `targetSize` on every batch. The collector relies on this to lower its target
 * during Kinesis→SQS failover, and to retry a single record at the larger `maxBytes`.
 *
 * Run against both engines, since `targetSize` is engine-independent (it lives only in
 * Compressor.Impl and the per-batch RewindableOutputStream).
 */
class CompressorTargetSizeSpec extends Specification with CatsEffect {
  import CompressionTestUtils.TestPayloadVersion

  def is = s2"""
  Varying targetSize across resets on one reused Compressor should
  ${engineTests("zstd", CompressorFactory.zstd(3), new Decompressor.Zstd(Int.MaxValue))}
  ${engineTests("gzip", CompressorFactory.gzip(6), new Decompressor.Gzip(Int.MaxValue))}
  """

  private def engineTests(
    name: String,
    factory: CompressorFactory,
    df: Decompressor.Factory
  ) = {
    def increase         = acceptsAtLargerTargetWhatItRejectedAtSmaller(factory, df)
    def decrease         = honoursReducedTargetOnLaterBatch(factory, df)
    def oscillate        = keepsEveryBatchCorrectWhenTargetOscillates(factory, df)
    def rejectThenShrink = clearsStateAcrossAReducingSizeChange(factory, df)

    s2"""
  $name
    accept at a larger target a record it rejected at a smaller target   $increase
    honour a reduced target on the next batch                            $decrease
    keep every batch correct and within its own target when it oscillates $oscillate
    leak nothing from a rejected large record into a later smaller batch $rejectThenShrink
  """
  }

  /**
   * The collector's single-record retry: a record too big for the (reduced) failover target is
   * rejected, then retried against the same compressor reset at the larger `maxBytes`. The rejected
   * batch is discarded, not emitted — per the "first rejected record yields an invalid stream"
   * contract (see ZstdCompressorSpec / GzipCompressorSpec) — so we reset and retry rather than
   * decoding it.
   */
  private def acceptsAtLargerTargetWhatItRejectedAtSmaller(
    factory: CompressorFactory,
    df: Decompressor.Factory
  ): IO[MatchResult[Any]] = {
    val record = randomBytes(5000, seed = 1) // incompressible: ~5000 bytes compressed

    factory.resource[IO].use { c =>
      IO {
        c.reset(TestPayloadVersion, targetSize = 100)
        val rejectedAtSmall = c.addRecord(record, 0, record.length)

        c.reset(TestPayloadVersion, targetSize = 100000)
        val acceptedAtLarge = c.addRecord(record, 0, record.length)
        val decoded         = decode(df, c.result)

        (rejectedAtSmall must beFalse: MatchResult[Any]) and
          (acceptedAtLarge must beTrue) and
          (decoded must haveSize(1)) and
          (decoded.head.sameElements(record) must beTrue)
      }
    }
  }

  /** Kinesis→SQS failover direction: the same compressor, reset smaller, admits fewer records. */
  private def honoursReducedTargetOnLaterBatch(
    factory: CompressorFactory,
    df: Decompressor.Factory
  ): IO[MatchResult[Any]] = {
    val records = List.tabulate(10)(i => randomBytes(500, seed = i)) // ~5000 bytes total, incompressible

    factory.resource[IO].use { c =>
      IO {
        c.reset(TestPayloadVersion, targetSize = 100000)
        val acceptedLarge = addAll(c, records)
        val decodedLarge  = decode(df, c.result)

        c.reset(TestPayloadVersion, targetSize = 1500)
        val acceptedSmall  = addAll(c, records)
        val smallResult    = c.result
        val smallResultLen = smallResult.remaining()
        val decodedSmall   = decode(df, smallResult)

        (acceptedLarge must haveSize(10): MatchResult[Any]) and
          (decodedLarge must haveSize(10)) and
          (acceptedSmall.size must beLessThan(10)) and
          (acceptedSmall.size must beGreaterThan(0)) and
          (smallResultLen must beLessThanOrEqualTo(1500)) and
          (sameRecords(decodedSmall, acceptedSmall) must beTrue)
      }
    }
  }

  /**
   * Many batches on one reused compressor with the target jumping around. Every batch must decode
   * to exactly what it accepted, and every frame must stay within that batch's own target.
   */
  private def keepsEveryBatchCorrectWhenTargetOscillates(
    factory: CompressorFactory,
    df: Decompressor.Factory
  ): IO[MatchResult[Any]] = {
    val targets = List(50000, 300, 50000, 400, 100000, 250)

    factory.resource[IO].use { c =>
      IO {
        val results = targets.zipWithIndex.map { case (target, batchIdx) =>
          val records = List.tabulate(6)(i => randomBytes(120, seed = batchIdx * 100 + i))
          c.reset(TestPayloadVersion, targetSize = target)
          val accepted = addAll(c, records)
          val bb       = c.result
          val len      = bb.remaining()
          val decoded  = decode(df, bb)
          (target, accepted, decoded, len)
        }

        results.foldLeft(ok: MatchResult[Any]) { case (acc, (target, accepted, decoded, len)) =>
          acc and
            (sameRecords(decoded, accepted) must beTrue) and
            (len must beLessThanOrEqualTo(target))
        }
      }
    }
  }

  /**
   * Reject a big record at a large target, then reset at a smaller target. The subsequent smaller
   * batch must be byte-identical to a fresh compressor's output for the same batch — proving the
   * rejected record (and, for zstd, its native ctx window) left nothing behind across the size
   * change.
   */
  private def clearsStateAcrossAReducingSizeChange(
    factory: CompressorFactory,
    df: Decompressor.Factory
  ): IO[MatchResult[Any]] = {
    val batch2 = List("alpha", "beta", "gamma", "delta").map(_.getBytes("UTF-8"))

    val reused: IO[Array[Byte]] =
      factory.resource[IO].use { c =>
        IO {
          c.reset(TestPayloadVersion, targetSize = 100000)
          val small = "small".getBytes("UTF-8")
          discard(c.addRecord(small, 0, small.length))
          val big = randomBytes(50000, seed = 7) // won't fit once we shrink
          discard(c.addRecord(big, 0, big.length))
          discard(c.result) // discard this batch

          c.reset(TestPayloadVersion, targetSize = 300)
          discard(addAll(c, batch2))
          toArray(c.result)
        }
      }

    val fresh: IO[Array[Byte]] =
      factory.resource[IO].use { c =>
        IO {
          c.reset(TestPayloadVersion, targetSize = 300)
          discard(addAll(c, batch2))
          toArray(c.result)
        }
      }

    (reused, fresh).mapN { (afterReuse, freshBytes) =>
      val decoded = decode(df, ByteBuffer.wrap(afterReuse))
      (afterReuse.toList must beEqualTo(freshBytes.toList)) and
        (sameRecords(decoded, batch2) must beTrue)
    }
  }

  // --- helpers ---

  private def addAll(c: Compressor, records: List[Array[Byte]]): List[Array[Byte]] =
    records.takeWhile(r => c.addRecord(r, 0, r.length))

  private def discard(a: Any): Unit = {
    val _ = a
  }

  private def randomBytes(size: Int, seed: Int): Array[Byte] = {
    val bytes = new Array[Byte](size)
    new Random(seed.toLong).nextBytes(bytes)
    bytes
  }

  private def sameRecords(a: List[Array[Byte]], b: List[Array[Byte]]): Boolean =
    a.length == b.length && a.zip(b).forall { case (x, y) => x.sameElements(y) }

  private def toArray(bb: ByteBuffer): Array[Byte] = {
    val a = new Array[Byte](bb.remaining())
    bb.get(a)
    a
  }

  private def decode(df: Decompressor.Factory, bb: ByteBuffer): List[Array[Byte]] =
    df.build(bb) match {
      case Decompressor.FactorySuccess(d, _) => drain(d)
      case other                             => throw new RuntimeException(s"Unexpected: $other")
    }

  @tailrec
  private def drain(d: Decompressor, acc: List[Array[Byte]] = Nil): List[Array[Byte]] =
    d.getNextRecord match {
      case Decompressor.Record(bytes) => drain(d, bytes :: acc)
      case Decompressor.EndOfRecords  => d.close(); acc.reverse
      case other                      => d.close(); throw new RuntimeException(s"Unexpected: $other")
    }
}
