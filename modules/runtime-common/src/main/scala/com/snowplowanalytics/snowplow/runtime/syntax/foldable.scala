/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime.syntax

import cats.implicits._
import cats.{Foldable, Monad}
import java.nio.ByteBuffer

trait FoldableExtensionSyntax {
  implicit final def snowplowFoldableSyntax[M[_]](foldable: Foldable[M]): FoldableExtensionOps[M] = new FoldableExtensionOps(foldable)
}

final class FoldableExtensionOps[M[_]](private val M: Foldable[M]) extends AnyVal {

  /**
   * Traversal over a List with an effect
   *
   * This is similar to a cats Traverse. But it is more efficient than cats Traverse because it does
   * not attempt to keep the order of the original list in the final result.
   *
   * This is helpful in many snowplow apps, where order of events is not important to us.
   *
   * Example:
   * {{{
   * Foldable[List].tranverseUnordered(events) { event =>
   *    IO.delay {
   *      transformEvent(event)
   *    }
   *  }
   * }}}
   */
  def traverseUnordered[F[_]: Monad, A, B](items: M[A])(f: A => F[B]): F[List[B]] =
    M.foldM(items, List.empty[B]) { case (acc, item) =>
      f(item).map(_ :: acc)
    }

  /**
   * Traversal over a List with an effect that produces success or failure
   *
   * This is helpful in many snowplow apps where we process events in batches, and each event might
   * produce a bad row. We typically want to handle the resulting bad rows separately from the
   * successes. And we don't care about the order of events.
   *
   * Example:
   * {{{
   *  Foldable[List].traverseUnordered(strings) { str =>
   *    IO.delay {
   *      Event.parse(str).toEither
   *    }
   *  }.map { case (badRows, events) =>
   *    // handle results
   *  }
   * }}}
   */
  def traverseSeparateUnordered[F[_]: Monad, A, B, C](items: M[A])(f: A => F[Either[B, C]]): F[(List[B], List[C])] =
    M.foldM(items, (List.empty[B], List.empty[C])) { case ((lefts, rights), item) =>
      f(item).map {
        case Left(b)  => (b :: lefts, rights)
        case Right(c) => (lefts, c :: rights)
      }
    }

  /**
   * Sum elements of a List
   *
   * Helpful in snowplow apps for summing the lengths of byte buffers
   *
   * Example:
   * {{{
   * Foldable[Chunk].sumBy(byteBuffers) { b =>
   *   b.limit() - b.position()
   * }
   * }}}
   */
  def sumBy[F[_], A](items: M[A])(f: A => Long): Long =
    M.foldLeft(items, 0L) { case (acc, item) =>
      acc + f(item)
    }

  /**
   * Sum total number of bytes in a list of byte buffers
   *
   * Example:
   * {{{
   * Foldable[Chunk].sumBytes(byteBuffers)
   * }}}
   */
  def sumBytes[F[_]](items: M[ByteBuffer]): Long =
    sumBy(items) { byteBuffer =>
      (byteBuffer.limit() - byteBuffer.position()).toLong
    }
}
