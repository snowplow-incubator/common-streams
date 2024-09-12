/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import cats.effect.{Async, Ref, Sync}
import cats.implicits._
import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sources.internal.{LowLevelEvents, LowLevelSource}
import fs2.{Chunk, Pull, Stream}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import software.amazon.kinesis.lifecycle.events.{ProcessRecordsInput, ShardEndedInput}
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import java.util.concurrent.{CountDownLatch, SynchronousQueue}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

object KinesisSource {

  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def build[F[_]: Async](config: KinesisSourceConfig): F[SourceAndAck[F]] =
    Ref.ofEffect(Sync[F].realTime).flatMap { liveness =>
      LowLevelSource.toSourceAndAck {
        new LowLevelSource[F, Map[String, Checkpointable]] {
          def stream: Stream[F, Stream[F, LowLevelEvents[Map[String, Checkpointable]]]] =
            kinesisStream(config, liveness)

          def checkpointer: KinesisCheckpointer[F] =
            new KinesisCheckpointer[F]()

          def lastLiveness: F[FiniteDuration] =
            liveness.get
        }
      }
    }

  private def kinesisStream[F[_]: Async](
    config: KinesisSourceConfig,
    liveness: Ref[F, FiniteDuration]
  ): Stream[F, Stream[F, LowLevelEvents[Map[String, Checkpointable]]]] = {
    val actionQueue = new SynchronousQueue[KCLAction]()
    for {
      _ <- Stream.resource(KCLScheduler.populateQueue[F](config, actionQueue))
      events <- Stream.emit(pullFromQueue(actionQueue, liveness).stream).repeat
    } yield events
  }

  private def pullFromQueue[F[_]: Sync](
    queue: SynchronousQueue[KCLAction],
    liveness: Ref[F, FiniteDuration]
  ): Pull[F, LowLevelEvents[Map[String, Checkpointable]], Unit] =
    Pull.eval(resolveNextAction(queue, liveness)).flatMap {
      case KCLAction.ProcessRecords(_, processRecordsInput) if processRecordsInput.records.asScala.isEmpty =>
        pullFromQueue[F](queue, liveness)
      case KCLAction.ProcessRecords(shardId, processRecordsInput) =>
        Pull.output1(provideNextChunk(shardId, processRecordsInput)).covary[F] *> pullFromQueue[F](queue, liveness)
      case KCLAction.ShardEnd(shardId, await, shardEndedInput) =>
        handleShardEnd[F](shardId, await, shardEndedInput) *> Pull.done
      case KCLAction.KCLError(t) =>
        Pull.eval(Logger[F].error(t)("Exception from Kinesis source")) *> Pull.raiseError[F](t)
    }

  private def resolveNextAction[F[_]: Sync](queue: SynchronousQueue[KCLAction], liveness: Ref[F, FiniteDuration]): F[KCLAction] = {
    val nextAction = Sync[F].delay(Option[KCLAction](queue.poll)).flatMap {
      case Some(action) => Sync[F].pure(action)
      case None         => Sync[F].interruptible(queue.take)
    }
    nextAction <* updateLiveness(liveness)
  }

  private def updateLiveness[F[_]: Sync](liveness: Ref[F, FiniteDuration]): F[Unit] =
    Sync[F].realTime.flatMap(now => liveness.set(now))

  private def provideNextChunk(shardId: String, input: ProcessRecordsInput) = {
    val chunk      = Chunk.javaList(input.records()).map(_.data())
    val lastRecord = input.records.asScala.last // last is safe because we handled the empty case above
    val checkpointable = Checkpointable.Record(
      new ExtendedSequenceNumber(lastRecord.sequenceNumber, lastRecord.subSequenceNumber),
      input.checkpointer
    )
    LowLevelEvents(chunk, Map[String, Checkpointable](shardId -> checkpointable), Some(lastRecord.approximateArrivalTimestamp))
  }

  private def handleShardEnd[F[_]: Sync](
    shardId: String,
    await: CountDownLatch,
    shardEndedInput: ShardEndedInput
  ) = {
    val checkpointable = Checkpointable.ShardEnd(shardEndedInput.checkpointer, await)
    val last           = LowLevelEvents(Chunk.empty, Map[String, Checkpointable](shardId -> checkpointable), None)
    Pull
      .eval(Logger[F].info(s"Ending this window of events early because reached the end of Kinesis shard $shardId"))
      .covaryOutput *>
      Pull.output1(last).covary[F]
  }

}
