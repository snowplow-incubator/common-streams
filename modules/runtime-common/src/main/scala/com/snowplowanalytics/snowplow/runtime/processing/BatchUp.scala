/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime.processing

import cats.{Foldable, Semigroup}
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
  trait Batchable[A] extends Semigroup[A] {
    def weightOf(a: A): Long
  }

  object Batchable {
    def apply[A](implicit batchable: Batchable[A]): Batchable[A] = batchable
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
  def withTimeout[F[_]: Async, A: Batchable](maxWeight: Long, maxDelay: FiniteDuration): Pipe[F, A, A] = {

    def go(timedPull: Pull.Timed[F, A], wasPending: Option[A]): Pull[F, A, Unit] =
      timedPull.uncons.flatMap {
        case None =>
          // Upstream finished cleanly. Emit whatever is pending and we're done.
          Pull.outputOption1[F, A](wasPending) *> Pull.done
        case Some((Left(_), next)) =>
          // Timer timed-out. Emit whatever is pending.
          Pull.outputOption1[F, A](wasPending) *> go(next, None)
        case Some((Right(chunk), next)) =>
          // Upstream emitted something to us. We might already have a pending element.
          combineByWeight(maxWeight, Chunk.fromOption(wasPending) ++ chunk) match {
            case CombineByWeightResult(None, toEmit) =>
              // We're emitting everything so cancel any existing timeout
              Pull.output[F, A](Chunk.from(toEmit)) *>
                next.timeout(Duration.Zero) *>
                go(next, None)
            case CombineByWeightResult(Some(notAtSize), toEmit) =>
              // We are not emitting everything; there will be a new pending element.
              val setTimeout = if (toEmit.nonEmpty || wasPending.isEmpty) {
                // There is no existing timeout on the pending element, so start a new timeout
                next.timeout(maxDelay)
              } else {
                // There must already by a timeout on the pending element
                Pull.pure(())
              }

              Pull.output[F, A](Chunk.from(toEmit)) *>
                setTimeout *>
                go(next, Some(notAtSize))
          }
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
  def noTimeout[F[_]: Sync, A: Batchable](maxWeight: Long): Pipe[F, A, A] = {
    def go(stream: Stream[F, A], unflushed: Option[A]): Pull[F, A, Unit] =
      stream.pull.uncons.flatMap {
        case None =>
          // Upstream finished cleanly. Emit whatever is pending and we're done.
          Pull.outputOption1[F, A](unflushed) *> Pull.done
        case Some((chunk, next)) =>
          val CombineByWeightResult(notAtSize, toEmit) = combineByWeight(maxWeight, Chunk.fromOption(unflushed) ++ chunk)
          Pull.output[F, A](Chunk.from(toEmit)) *>
            go(next, notAtSize)
      }

    in => go(in, None).stream
  }

  /**
   * The result of combining a chunk of `A`s, while not exceeding total weight.
   *
   * @param notAtSize
   *   Optionally an `A` that does not yet exceed the maximum allowed size. We should not emit this
   *   `A` but instead wait in case we can combine it with other `A`s later.
   * @param toEmit
   *   The combined `A`s which meet size requirements. These should be emitted downstream because we
   *   cannot combine them with anything more.
   */
  private case class CombineByWeightResult[A](notAtSize: Option[A], toEmit: Vector[A])

  /**
   * Combine a chunk of `A`s, while not exceeding the max allowed weight
   *
   * @param maxWeight
   *   the maximum allowed weight (e.g. max allowed number of bytes)
   * @param chunk
   *   the `A`s we need to combine into larger `A`s.
   * @return
   *   The result of combining `A`s
   */
  private def combineByWeight[A: Batchable](maxWeight: Long, chunk: Chunk[A]): CombineByWeightResult[A] =
    Foldable[Chunk].foldLeft(chunk, CombineByWeightResult[A](None, Vector.empty)) {
      case (CombineByWeightResult(None, toEmit), next) =>
        if (Batchable[A].weightOf(next) >= maxWeight)
          CombineByWeightResult(None, toEmit :+ next)
        else
          CombineByWeightResult(Some(next), toEmit)
      case (CombineByWeightResult(Some(notAtSize), toEmit), next) =>
        val nextWeight = Batchable[A].weightOf(next)
        if (nextWeight >= maxWeight)
          CombineByWeightResult(None, toEmit :+ notAtSize :+ next)
        else {
          val notAtSizeWeight = Batchable[A].weightOf(notAtSize)
          if (nextWeight + notAtSizeWeight > maxWeight)
            CombineByWeightResult(Some(next), toEmit :+ notAtSize)
          else if (nextWeight + notAtSizeWeight === maxWeight)
            CombineByWeightResult(None, toEmit :+ (notAtSize |+| next))
          else
            CombineByWeightResult(Some(notAtSize |+| next), toEmit)
        }
    }
}
