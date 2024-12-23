/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import cats.effect.{Async, Sync}
import cats.implicits._
import cats.effect.implicits._
import org.typelevel.log4cats.Logger
import retry.syntax.all._
import software.amazon.kinesis.exceptions.{ShutdownException, ThrottlingException}
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import com.snowplowanalytics.snowplow.sources.internal.Checkpointer
import com.snowplowanalytics.snowplow.kinesis.{BackoffPolicy, Retries}

import java.util.concurrent.CountDownLatch

private class KinesisCheckpointer[F[_]: Async: Logger](throttledBackoffPolicy: BackoffPolicy)
    extends Checkpointer[F, Map[String, Checkpointable]] {

  private val retryPolicy = Retries.forThrottling[F](throttledBackoffPolicy)

  override val empty: Map[String, Checkpointable] = Map.empty

  override def combine(x: Map[String, Checkpointable], y: Map[String, Checkpointable]): Map[String, Checkpointable] =
    x |+| y

  override def ack(c: Map[String, Checkpointable]): F[Unit] =
    c.toList.parTraverse_ {
      case (shardId, Checkpointable.Record(extendedSequenceNumber, checkpointer)) =>
        checkpointRecord(shardId, extendedSequenceNumber, checkpointer)
      case (shardId, Checkpointable.ShardEnd(checkpointer, release)) =>
        checkpointShardEnd(shardId, checkpointer, release)
    }

  override def nack(c: Map[String, Checkpointable]): F[Unit] =
    Sync[F].unit

  private def checkpointShardEnd(
    shardId: String,
    checkpointer: RecordProcessorCheckpointer,
    release: CountDownLatch
  ) =
    Logger[F].debug(s"Checkpointing shard $shardId at SHARD_END") *>
      Sync[F].blocking(checkpointer.checkpoint()).recoverWith(ignoreShutdownExceptions(shardId)) *>
      Sync[F].delay(release.countDown())

  private def checkpointRecord(
    shardId: String,
    extendedSequenceNumber: ExtendedSequenceNumber,
    checkpointer: RecordProcessorCheckpointer
  ) =
    Logger[F].debug(s"Checkpointing shard $shardId at $extendedSequenceNumber") *>
      Sync[F]
        .blocking(
          checkpointer.checkpoint(extendedSequenceNumber.sequenceNumber, extendedSequenceNumber.subSequenceNumber)
        )
        .recoverWith(ignoreShutdownExceptions(shardId))
        .retryingOnSomeErrors(
          policy = retryPolicy,
          isWorthRetrying = {
            case _: ThrottlingException => true.pure[F]
            case _                      => false.pure[F]
          },
          onError = { case (_, retryDetails) =>
            Logger[F].warn(
              s"Exceeded DynamoDB provisioned throughput. Checkpointing will be retried. (${retryDetails.retriesSoFar} retries so far)"
            )
          }
        )

  private def ignoreShutdownExceptions(shardId: String): PartialFunction[Throwable, F[Unit]] = { case _: ShutdownException =>
    // The ShardRecordProcessor instance has been shutdown. This just means another KCL
    // worker has stolen our lease. It is expected during autoscaling of instances, and is
    // safe to ignore.
    Logger[F].warn(s"Skipping checkpointing of shard $shardId because this worker no longer owns the lease")
  }
}
