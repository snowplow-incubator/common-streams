/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams

/**
 * A common interface over the external sinks that Snowplow can write to
 *
 * Implementations of this trait are provided by the sinks library (e.g. kinesis, kafka, pubsub)
 */
trait Sink[F[_]] {

  /**
   * Writes a batch of events to the external sink, handling partition keys and message attributes
   */
  def sink(batch: ListOfList[Sinkable]): F[Unit]

  /** Writes a batch of events to the sink using an empty partition key and attributes */
  def sinkSimple(batch: ListOfList[Array[Byte]]): F[Unit] =
    sink(batch.mapUnordered(Sinkable(_, None, Map.empty)))

}

object Sink {

  def apply[F[_]](f: ListOfList[Sinkable] => F[Unit]): Sink[F] = new Sink[F] {
    def sink(batch: ListOfList[Sinkable]): F[Unit] = f(batch)
  }
}
