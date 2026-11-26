/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.internal

import cats.{Applicative, Monoid}

/**
 * Checkpoints messages to the input stream so that we don't read them again. A single checkpointer
 * might be responsible for multiple incoming messages.
 * @tparam F
 *   An effect
 * @tparam C
 *   Source-specific type describing the outstading messages
 */
private[streams] trait Checkpointer[F[_], C] extends Monoid[C] {

  /**
   * Trigger checkpointing. Must be called only when all message processing is complete.
   *
   * The generic layer never calls `ack` after the source's streams are finalized, except that a
   * stream which ended by itself or raised an error may already have been finalized. See the
   * contract on `LowLevelSource.stream`.
   */
  def ack(c: C): F[Unit]

  /**
   * Nack messages the event processor never reported as processed. Called at most once, when the
   * stream returned by `SourceAndAck.stream` ends, possibly with nothing to nack. It is not called
   * if that stream is cancelled before it starts.
   *
   * The generic layer calls it after the windows have finished, and before the source's streams are
   * finalized, except that a stream which ended by itself or raised an error may already have been
   * finalized. See the contract on `LowLevelSource.stream`.
   */
  def nack(c: C): F[Unit]

}

private[streams] object Checkpointer {

  /** A simple implementation that does ack only and no nacks */
  def acksOnly[F[_]: Applicative, C: Monoid](f: C => F[Unit]): Checkpointer[F, C] =
    new Checkpointer[F, C] {
      override def ack(c: C): F[Unit]     = f(c)
      override def nack(c: C): F[Unit]    = Applicative[F].unit
      override def empty: C               = Monoid[C].empty
      override def combine(x: C, y: C): C = Monoid[C].combine(x, y)
    }

}
