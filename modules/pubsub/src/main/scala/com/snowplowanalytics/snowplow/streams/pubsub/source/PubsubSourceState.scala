/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub.source

import cats.effect.kernel.Unique

import java.time.Instant

/**
 * State shared between the pubsub source, the checkpointer, and the release of un-emitted batches
 * on shutdown.
 *
 * It holds every batch pulled from pubsub that has not yet been acked or nacked, keyed by its
 * token. Each method is a pure transition that returns the new state alongside a result, so that it
 * can be passed to `Ref.modify` and run as a single atomic step. Atomicity is what guarantees that
 * a batch is never both handed downstream and released back to pubsub: a batch is released only
 * from `buffered`, and handed over only while the state is [[PubsubSourceState.Pulling]].
 */
private sealed trait PubsubSourceState {

  import PubsubSourceState._

  /**
   * Adds a newly received batch to `buffered`. When [[Released]], it rejects the batch and leaves
   * the state as-is, and the caller must nack the batch.
   */
  def receive(token: Unique.Token, batch: PubsubBatchState): (PubsubSourceState, Boolean) =
    this match {
      case Pulling(buffered, emitted) => (Pulling(buffered + (token -> batch), emitted), true)
      case Released(_)                => (this, false)
    }

  /**
   * Decides whether the item carrying these batches is handed downstream. While [[Pulling]] the
   * batches move from `buffered` to `emitted`, and the result is true. When [[Released]] the result
   * is false, and the caller must stop handing anything downstream. An empty `tokens` stands for an
   * item without batches, such as a pull that returned nothing.
   */
  def handOver(tokens: Vector[Unique.Token]): (PubsubSourceState, Boolean) =
    this match {
      case Pulling(buffered, emitted) =>
        val moved = tokens.flatMap(t => buffered.get(t).map(t -> _))
        (Pulling(buffered -- tokens, emitted ++ moved), true)
      case Released(_) =>
        (this, false)
    }

  /** Moves to [[Released]], and returns the `buffered` batches so the caller can nack them */
  def release: (PubsubSourceState, Vector[PubsubBatchState]) =
    this match {
      case Pulling(buffered, emitted) => (Released(emitted), buffered.values.toVector)
      case Released(_)                => (this, Vector.empty)
    }

  /** Removes the given batches, so the caller can ack or nack them */
  def remove(tokens: Vector[Unique.Token]): (PubsubSourceState, Vector[PubsubBatchState]) =
    this match {
      case Pulling(buffered, emitted) =>
        (Pulling(buffered -- tokens, emitted -- tokens), tokens.flatMap(t => buffered.get(t).orElse(emitted.get(t))))
      case Released(emitted) =>
        (Released(emitted -- tokens), tokens.flatMap(emitted.get))
    }

  /** Removes every batch, buffered or emitted, so the caller can nack them */
  def removeAll: (PubsubSourceState, Vector[PubsubBatchState]) =
    this match {
      case Pulling(buffered, emitted) => (Pulling(Map.empty, Map.empty), buffered.values.toVector ++ emitted.values)
      case Released(emitted)          => (Released(Map.empty), emitted.values.toVector)
    }

  /**
   * Moves the deadline of every batch whose deadline is before `minAllowedDeadline` to
   * `newDeadline`, and returns those batches, with their old deadlines, so the caller can extend
   * them in pubsub
   */
  def extendDeadlines(minAllowedDeadline: Instant, newDeadline: Instant): (PubsubSourceState, Vector[PubsubBatchState]) = {
    def extend(m: Map[Unique.Token, PubsubBatchState]): (Map[Unique.Token, PubsubBatchState], Vector[PubsubBatchState]) = {
      val toExtend = m.filter { case (_, b) => b.currentDeadline.isBefore(minAllowedDeadline) }
      (m ++ toExtend.map { case (t, b) => t -> b.copy(currentDeadline = newDeadline) }, toExtend.values.toVector)
    }
    this match {
      case Pulling(buffered, emitted) =>
        val (b, extendedB) = extend(buffered)
        val (e, extendedE) = extend(emitted)
        (Pulling(b, e), extendedB ++ extendedE)
      case Released(emitted) =>
        val (e, extended) = extend(emitted)
        (Released(e), extended)
    }
  }

  def isReleased: Boolean =
    this match {
      case Pulling(_, _) => false
      case Released(_)   => true
    }
}

private object PubsubSourceState {

  /**
   * The source is pulling from pubsub and handing batches downstream
   *
   * @param buffered
   *   Batches pulled from pubsub but still held in the source's own buffers
   * @param emitted
   *   Batches handed downstream, which the checkpointer acks or nacks
   */
  case class Pulling(buffered: Map[Unique.Token, PubsubBatchState], emitted: Map[Unique.Token, PubsubBatchState]) extends PubsubSourceState

  /**
   * Un-emitted batches have been released back to pubsub. Nothing more is buffered or handed
   * downstream, and the state never returns to [[Pulling]].
   *
   * @param emitted
   *   Batches handed downstream before the release, which the checkpointer acks or nacks
   */
  case class Released(emitted: Map[Unique.Token, PubsubBatchState]) extends PubsubSourceState

  val initial: PubsubSourceState = Pulling(Map.empty, Map.empty)
}
