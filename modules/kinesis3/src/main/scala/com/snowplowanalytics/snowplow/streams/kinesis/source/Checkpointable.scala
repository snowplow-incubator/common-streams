/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kinesis.source

import cats.implicits._
import cats.{Order, Semigroup}
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import java.util.concurrent.CountDownLatch

private sealed trait Checkpointable {
  def extendedSequenceNumber: ExtendedSequenceNumber
}

private object Checkpointable {
  final case class Record(extendedSequenceNumber: ExtendedSequenceNumber, checkpointer: RecordProcessorCheckpointer) extends Checkpointable

  final case class ShardEnd(checkpointer: RecordProcessorCheckpointer, release: CountDownLatch) extends Checkpointable {
    override def extendedSequenceNumber: ExtendedSequenceNumber = ExtendedSequenceNumber.SHARD_END
  }

  implicit def checkpointableOrder: Order[Checkpointable] = Order.from { case (a, b) =>
    a.extendedSequenceNumber.compareTo(b.extendedSequenceNumber)
  }

  implicit def checkpointableSemigroup: Semigroup[Checkpointable] = new Semigroup[Checkpointable] {
    def combine(x: Checkpointable, y: Checkpointable): Checkpointable =
      x.max(y)
  }
}
