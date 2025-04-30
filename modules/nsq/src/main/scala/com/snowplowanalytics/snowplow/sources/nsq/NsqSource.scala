/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.nsq

import cats.implicits._
import cats.effect.{Async, Resource, Sync}
import cats.effect.std.{Dispatcher, Queue}

import fs2.{Chunk, Stream}

import com.sproutsocial.nsq.{Message, MessageHandler, Subscriber}

import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.ByteBuffer

import scala.concurrent.duration.{Duration, DurationLong, FiniteDuration}

import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sources.internal.{Checkpointer, LowLevelEvents, LowLevelSource}

object NsqSource {

  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def build[F[_]: Async](config: NsqSourceConfig): F[SourceAndAck[F]] =
    LowLevelSource.toSourceAndAck {
      new LowLevelSource[F, Vector[Message]] {
        def checkpointer: Checkpointer[F, Vector[Message]] = new NsqCheckpointer()

        def stream: fs2.Stream[F, fs2.Stream[F, Option[LowLevelEvents[Vector[Message]]]]] = nsqStream(config)

        def debounceCheckpoints: FiniteDuration = Duration.Zero
      }
    }

  private def nsqStream[F[_]: Async](
    config: NsqSourceConfig
  ): Stream[F, Stream[F, Option[LowLevelEvents[Vector[Message]]]]] =
    for {
      _ <- Stream.eval(Logger[F].info(s"Starting processing of topic ${config.topic}"))
      queue <- Stream.eval(Queue.bounded[F, Message](config.maxBufferQueueSize))
      dispatcher <- Stream.resource(Dispatcher.sequential(await = true))
      _ <- Stream.resource(startSubscriber(config, queue, dispatcher))
    } yield Stream
      .fromQueueUnterminated(queue)
      .chunks
      .map { chunk =>
        Some(LowLevelEvents(chunk.map(m => ByteBuffer.wrap(m.getData)), chunk.toVector, Some(earliestTimestampOfMessages(chunk))))
      }
      .mergeHaltL(Stream.awakeDelay(10.seconds).map(_ => None).repeat) // keepalives
      .onFinalize {
        Logger[F].info(s"Stopping processing of topic ${config.topic}")
      }

  private def earliestTimestampOfMessages(messages: Chunk[Message]): FiniteDuration =
    messages.iterator.map(_.getTimestamp).min.nanos

  private def startSubscriber[F[_]: Sync](
    config: NsqSourceConfig,
    queue: Queue[F, Message],
    dispatcher: Dispatcher[F]
  ): Resource[F, Subscriber] =
    Resource.make {
      Sync[F].delay {
        val subscriber = new Subscriber(s"${config.lookupHost}:${config.lookupPort}")
        subscriber.subscribe(
          config.topic,
          config.channel,
          new MessageHandler {
            override def accept(msg: Message): Unit = {
              val enqueue = queue.offer(msg)
              dispatcher.unsafeRunAndForget(enqueue)
            }
          }
        )
        subscriber
      }
    }(s => Sync[F].blocking(s.stop()))

}
