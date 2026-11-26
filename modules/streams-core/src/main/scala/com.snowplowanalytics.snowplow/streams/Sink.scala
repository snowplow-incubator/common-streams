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
   *
   * Pass `Sinkable`s that are already built. A view over a strictly mapped `ListOfList` (e.g.
   * `batch.map(serialize).view`) is the cheap way to do that. Don't leave expensive transformations
   * inside a lazy view: the sink decides where and how often `batch` is traversed, so deferred work
   * may run on the sink's own threads, or more than once.
   */
  def sink(batch: Iterable[Sinkable]): F[Unit]

  /**
   * Writes a batch of events to the sink using an empty partition key and attributes
   *
   * The `Sinkable`s are built lazily, through a view over `batch`, so no intermediate collection is
   * allocated.
   */
  def sinkSimple(batch: Iterable[Array[Byte]]): F[Unit] =
    sink(batch.view.map(Sinkable(_, None, Map.empty)))

  /**
   * Ping the external resource to check it is healthy and ready to receive events
   *
   * The sink is expected to probe the external resource just one time, e.g. by sending a HTTP
   * request, and then return a result
   *
   * This health check can yield any of three results:
   *
   *   1. `true` (wrapped in `F`). This is the only result that indicates the sink is healthy.
   *
   * 2. `false` (wrapped in `F`). This means we successfully probed the external resource, but it
   * tells us the external resource is not ready to receive messages.
   *
   * 3. An exception raised via the `F`.
   *
   * An application calling `isHealthy` should check for all types of result: true / false /
   * exception
   */
  def isHealthy: F[Boolean]

}
