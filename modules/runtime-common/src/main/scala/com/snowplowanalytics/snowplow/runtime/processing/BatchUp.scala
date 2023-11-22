/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime.processing

import cats.Foldable
import cats.implicits._
import cats.effect.{Async, Sync}
import fs2.{Chunk, Pipe, Pull, Stream}

import scala.concurrent.duration.{Duration, FiniteDuration}

/** Methods for batching-up small items into larger items in a FS2 streaming app */
object BatchUp {

  /**
   * Provides methods needed to combine items of type A, while not exceeding an overal "weight" of
   * the combined values.
   *
   * @note
   *   typically "weight" means "number of bytes". But it could also be any other measurement.
   */
  trait Batchable[A, B] {
    def weightOf(a: A): Long
    def single(a: A): B
    def combine(b: B, a: A): B
  }

  object Batchable {
    def apply[A, B](implicit batchable: Batchable[A, B]): Batchable[A, B] = batchable
  }

  /**
   * A FS2 pipe tha batches up items of type A into larger itmes, while not exceeding an overal
   * "weight" of the combined values.
   *
   * In Snowplow apps, `A` is typically a batch of events. And "weight" is typically the total
   * number of bytes in the batch.
   *
   * @param maxWeight
   *   The maximum allowed "weight" of the combined values. Typically this means maximum allowed
   *   number of bytes.
   * @param maxDelay
   *   The maximum time we may hold on to a pending `A` while waiting to combine it with other
   *   incoming `A`s. If we don't receive another `A` within this time limit then we must emit the
   *   pending value.
   * @return
   *   A FS2 Pipe which combines small `A`s to bigger `A`s.
   */
  def withTimeout[F[_]: Async, A, B: Batchable[A, *]](maxWeight: Long, maxDelay: FiniteDuration): Pipe[F, A, B] = {

    def go(timedPull: Pull.Timed[F, A], wasPending: Option[BatchWithWeight[B]]): Pull[F, B, Unit] =
      timedPull.uncons.flatMap {
        case None =>
          // Upstream finished cleanly. Emit whatever is pending and we're done.
          Pull.outputOption1[F, B](wasPending.map(_.value)) *> Pull.done
        case Some((Left(_), next)) =>
          // Timer timed-out. Emit whatever is pending.
          Pull.outputOption1[F, B](wasPending.map(_.value)) *> go(next, None)
        case Some((Right(chunk), next)) =>
          // Upstream emitted something to us. We might already have a pending element.
          val result: CombineByWeightResult[B] = combineByWeight[A, B](maxWeight, wasPending, chunk)
          Pull.output[F, B](Chunk.from(result.toEmit)) *>
            handleTimerReset(wasPending, result, next, maxDelay) *>
            go(next, result.doNotEmitYet)
      }

    in =>
      in.pull.timed { timedPull =>
        go(timedPull, None)
      }.stream
  }

  /**
   * A FS2 pipe tha batches up items of type A into larger itmes, while not exceeding an overal
   * "weight" of the combined values.
   *
   * In Snowplow apps, `A` is typically a batch of events. And "weight" is typically the total
   * number of bytes in the batch.
   *
   * This Pipe has no timeout: if an undersized `A` is pending, it will try to wait forever for a
   * new `A` to join with the pending one. If we never receive another `A` from upstream, then the
   * pending `A` will not be emitted until the upstream Stream ends.
   *
   * As such, in Snowplow apps this should only be used in windowing-applications where the upstream
   * Stream is periodically closed and re-started.
   *
   * @param maxWeight
   *   The maximum allowed "weight" of the combined values. Typically this means maximum allowed
   *   number of bytes.
   * @return
   *   A FS2 Pipe which combines small `A`s to bigger `A`s.
   */
  def noTimeout[F[_]: Sync, A, B: Batchable[A, *]](maxWeight: Long): Pipe[F, A, B] = {
    def go(stream: Stream[F, A], unflushed: Option[BatchWithWeight[B]]): Pull[F, B, Unit] =
      stream.pull.uncons.flatMap {
        case None =>
          // Upstream finished cleanly. Emit whatever is pending and we're done.
          Pull.outputOption1[F, B](unflushed.map(_.value)) *> Pull.done
        case Some((chunk, next)) =>
          val result = combineByWeight(maxWeight, unflushed, chunk)
          Pull.output[F, B](Chunk.from(result.toEmit)) *>
            go(next, result.doNotEmitYet)
      }

    in => go(in, None).stream
  }

  private case class BatchWithWeight[B](value: B, weight: Long)

  /**
   * The result of try to combine a chunk of `A`s, while not exceeding total weight.
   *
   * @param doNotEmitYet
   *   Optionally a batch `B` that does not yet exceed the maximum allowed size. We should not emit
   *   this `B` but instead wait in case we can combine it with other `A`s later.
   * @param toEmit
   *   The combined `B`s which meet size requirements. These should be emitted downstream because we
   *   cannot combine them with anything more.
   */
  private case class CombineByWeightResult[B](
    doNotEmitYet: Option[BatchWithWeight[B]],
    toEmit: Vector[B]
  )

  /**
   * Combine a chunk of `A`s, while not exceeding the max allowed weight
   *
   * @param maxWeight
   *   the maximum allowed weight (e.g. max allowed number of bytes)
   * @param notAtSize
   *   optionally a batch we have pending because it was not yet at size
   * @param chunk
   *   the `A`s we need to combine into larger `A`s.
   * @return
   *   The result of combining `A`s
   */
  private def combineByWeight[A, B](
    maxWeight: Long,
    notAtSize: Option[BatchWithWeight[B]],
    chunk: Chunk[A]
  )(implicit B: Batchable[A, B]
  ): CombineByWeightResult[B] =
    Foldable[Chunk].foldLeft(chunk, CombineByWeightResult[B](notAtSize, Vector.empty)) {
      case (CombineByWeightResult(None, toEmit), next) =>
        val nextWeight = B.weightOf(next)
        if (nextWeight >= maxWeight)
          CombineByWeightResult(None, toEmit :+ B.single(next))
        else
          CombineByWeightResult(Some(BatchWithWeight(B.single(next), nextWeight)), toEmit)
      case (CombineByWeightResult(Some(notAtSize), toEmit), next) =>
        val nextWeight = B.weightOf(next)
        if (nextWeight >= maxWeight)
          CombineByWeightResult(None, toEmit :+ notAtSize.value :+ B.single(next))
        else {
          val combinedWeight = nextWeight + notAtSize.weight
          if (combinedWeight > maxWeight)
            CombineByWeightResult(Some(BatchWithWeight(B.single(next), nextWeight)), toEmit :+ notAtSize.value)
          else if (combinedWeight === maxWeight)
            CombineByWeightResult(None, toEmit :+ B.combine(notAtSize.value, next))
          else
            CombineByWeightResult(Some(BatchWithWeight(B.combine(notAtSize.value, next), combinedWeight)), toEmit)
        }
    }

  /**
   * Resets the timeout if needed.
   *
   * @param wasPending
   *   whatever was already pending before we received a new chunk to emit. If this is non-empty
   *   then there must be an existing timeout already set.
   * @param result
   *   the result of combining the new chunk with any pending chunk. Tells us what will be emitted
   *   and what will be pending due to not yet at size.
   * @param timedPull
   *   the TimedPull that manages timeouts
   * @param maxDelay
   *   value to use for a new timeout, if needed
   */
  private def handleTimerReset[F[_], A, B](
    wasPending: Option[BatchWithWeight[B]],
    result: CombineByWeightResult[B],
    timedPull: Pull.Timed[F, A],
    maxDelay: FiniteDuration
  ): Pull[F, Nothing, Unit] =
    if (result.doNotEmitYet.isEmpty) {
      // We're emitting everything so cancel any existing timeout
      timedPull.timeout(Duration.Zero)
    } else if (result.toEmit.nonEmpty || wasPending.isEmpty) {
      // There is no existing timeout on the pending element, so start a new timeout
      timedPull.timeout(maxDelay)
    } else {
      // There must already by a timeout on the pending element
      Pull.pure(())
    }
}
