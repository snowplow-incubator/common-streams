/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kinesis.source

import cats.effect.{Async, Sync}
import cats.data.NonEmptyList
import cats.implicits._
import fs2.{Chunk, Pull, Stream}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.kinesis.lifecycle.events.{ProcessRecordsInput, ShardEndedInput}
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.jdk.CollectionConverters._

import com.snowplowanalytics.snowplow.streams.SourceAndAck
import com.snowplowanalytics.snowplow.streams.internal.{LowLevelEvents, LowLevelSource}
import com.snowplowanalytics.snowplow.streams.kinesis.KinesisSourceConfig

private[kinesis] object KinesisSource {

  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def build[F[_]: Async](config: KinesisSourceConfig, client: SdkAsyncHttpClient): F[SourceAndAck[F]] =
    LowLevelSource.toSourceAndAck {
      new LowLevelSource[F, Map[String, Checkpointable]] {
        def stream: Stream[F, Stream[F, Option[LowLevelEvents[Map[String, Checkpointable]]]]] =
          kinesisStream(config, client)

        def checkpointer: KinesisCheckpointer[F] =
          new KinesisCheckpointer[F](config.checkpointThrottledBackoffPolicy)

        def debounceCheckpoints: FiniteDuration = config.debounceCheckpoints
      }
    }

  private def kinesisStream[F[_]: Async](
    config: KinesisSourceConfig,
    client: SdkAsyncHttpClient
  ): Stream[F, Stream[F, Option[LowLevelEvents[Map[String, Checkpointable]]]]] = {
    val actionQueue = new LinkedBlockingQueue[KCLAction]()
    for {
      _ <- Stream.resource(KCLScheduler.populateQueue[F](config, actionQueue, client))
      events <- Stream.emit(pullFromQueueAndEmit(actionQueue).stream).repeat
    } yield events
  }

  private def pullFromQueueAndEmit[F[_]: Sync](
    queue: LinkedBlockingQueue[KCLAction]
  ): Pull[F, Option[LowLevelEvents[Map[String, Checkpointable]]], Unit] =
    Pull.eval(pullFromQueue(queue)).flatMap { actions =>
      val toEmit = actions.traverse {
        case KCLAction.ProcessRecords(_, await, processRecordsInput) if processRecordsInput.records.asScala.isEmpty =>
          Pull.eval(Sync[F].delay(await.countDown())) >> Pull.output1(None)
        case KCLAction.ProcessRecords(shardId, await, processRecordsInput) =>
          Pull.eval(Sync[F].delay(await.countDown())) >> Pull.output1(Some(provideNextChunk(shardId, processRecordsInput))).covary[F]
        case KCLAction.ShardEnd(shardId, await, shardEndedInput) =>
          // Do not call `await.countDown()` yet. It must be released later by the checkpointer.
          handleShardEnd[F](shardId, await, shardEndedInput)
        case KCLAction.KCLError(t, await) =>
          Pull.eval(Sync[F].delay(await.countDown())) >> Pull.eval(Logger[F].error(t)("Exception from Kinesis source")) >> Pull
            .raiseError[F](t)
      }
      val hasShardEnd = actions.exists {
        case _: KCLAction.ShardEnd => true
        case _: KCLAction          => false
      }
      if (hasShardEnd) {
        val log = Logger[F].info {
          actions
            .collect { case KCLAction.ShardEnd(shardId, _, _) =>
              shardId
            }
            .mkString("Ending this window of events early because reached the end of Kinesis shards: ", ",", "")
        }
        Pull.eval(log).covaryOutput *> toEmit *> Pull.done
      } else
        toEmit *> pullFromQueueAndEmit(queue)
    }

  private def pullFromQueue[F[_]: Sync](queue: LinkedBlockingQueue[KCLAction]): F[NonEmptyList[KCLAction]] =
    for {
      head <- resolveNextAction(queue)
      tail <- resolveAllActions(queue)
    } yield NonEmptyList(head, tail)

  /** Always returns a `KCLAction`, possibly waiting until one is available */
  private def resolveNextAction[F[_]: Sync](queue: LinkedBlockingQueue[KCLAction]): F[KCLAction] =
    Sync[F].delay(Option[KCLAction](queue.poll)).flatMap {
      case Some(action) => Sync[F].pure(action)
      case None         => Sync[F].interruptible(queue.take)
    }

  /** Returns immediately, but the `List[KCLAction]` might be empty */
  private def resolveAllActions[F[_]: Sync](queue: LinkedBlockingQueue[KCLAction]): F[List[KCLAction]] =
    for {
      ret <- Sync[F].delay(new java.util.ArrayList[KCLAction]())
      _ <- Sync[F].delay(queue.drainTo(ret))
    } yield ret.asScala.toList

  private def provideNextChunk(shardId: String, input: ProcessRecordsInput) = {
    val chunk       = Chunk.javaList(input.records()).map(_.data())
    val lastRecord  = input.records.asScala.last // last is safe because we handled the empty case above
    val firstRecord = input.records.asScala.head
    val checkpointable = Checkpointable.Record(
      new ExtendedSequenceNumber(lastRecord.sequenceNumber, lastRecord.subSequenceNumber),
      input.checkpointer
    )
    LowLevelEvents(
      chunk,
      Map[String, Checkpointable](shardId -> checkpointable),
      Some(firstRecord.approximateArrivalTimestamp.toEpochMilli.millis)
    )
  }

  private def handleShardEnd[F[_]](
    shardId: String,
    await: CountDownLatch,
    shardEndedInput: ShardEndedInput
  ): Pull[F, Option[LowLevelEvents[Map[String, Checkpointable]]], Unit] = {
    val checkpointable = Checkpointable.ShardEnd(shardEndedInput.checkpointer, await)
    val last           = LowLevelEvents(Chunk.empty, Map[String, Checkpointable](shardId -> checkpointable), None)
    Pull.output1(Some(last))
  }

}
