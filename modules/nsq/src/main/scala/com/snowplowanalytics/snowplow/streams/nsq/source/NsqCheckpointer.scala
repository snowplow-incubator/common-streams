/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.nsq.source

import cats.implicits._
import cats.effect.Async

import com.sproutsocial.nsq.Message

import com.snowplowanalytics.snowplow.streams.internal.Checkpointer

private class NsqCheckpointer[F[_]: Async] extends Checkpointer[F, Vector[Message]] {

  override def combine(x: Vector[Message], y: Vector[Message]): Vector[Message] =
    x |+| y

  override def empty: Vector[Message] = Vector.empty

  override def ack(messages: Vector[Message]): F[Unit] =
    messages.traverse_(m => Async[F].delay(m.finish()))

  override def nack(messages: Vector[Message]): F[Unit] =
    messages.traverse_(m => Async[F].delay(m.requeue()))
}
