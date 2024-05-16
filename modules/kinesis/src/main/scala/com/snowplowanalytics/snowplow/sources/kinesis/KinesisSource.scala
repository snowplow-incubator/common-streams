/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import cats.{Applicative, Semigroup}
import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.implicits._
import cats.implicits._
import fs2.Stream
import fs2.aws.kinesis.{CommittableRecord, Kinesis, KinesisConsumerSettings}
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.{InitialPositionInStream, InitialPositionInStreamExtended}

import java.net.URI
import java.util.Date
import java.util.concurrent.Semaphore
import scala.concurrent.duration.FiniteDuration

// kinesis
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.exceptions.ShutdownException
import software.amazon.kinesis.metrics.MetricsLevel
import software.amazon.kinesis.processor.{ShardRecordProcessorFactory, SingleStreamTracker}
import software.amazon.kinesis.retrieval.fanout.FanOutConfig
import software.amazon.kinesis.retrieval.polling.PollingConfig

// snowplow
import com.snowplowanalytics.snowplow.sources.internal.{Checkpointer, LowLevelEvents, LowLevelSource}
import com.snowplowanalytics.snowplow.sources.SourceAndAck

object KinesisSource {

  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def build[F[_]: Async](config: KinesisSourceConfig): F[SourceAndAck[F]] =
    Ref.ofEffect(Sync[F].realTime).flatMap { livenessRef =>
      LowLevelSource.toSourceAndAck(lowLevel(config, livenessRef))
    }

  private type KinesisCheckpointer[F[_]] = Checkpointer[F, Map[String, KinesisMetadata[F]]]

  private implicit class RichCommitableRecord(val cr: CommittableRecord) extends AnyVal {
    def toMetadata[F[_]: Sync]: KinesisMetadata[F] =
      KinesisMetadata(cr.shardId, cr.sequenceNumber, cr.isLastInShard, cr.lastRecordSemaphore, cr.checkpoint)
  }

  private final case class KinesisMetadata[F[_]](
    shardId: String,
    sequenceNumber: String,
    isLastInShard: Boolean,
    lastRecordSemaphore: Semaphore,
    ack: F[Unit]
  )

  private def lowLevel[F[_]: Async](
    config: KinesisSourceConfig,
    livenessRef: Ref[F, FiniteDuration]
  ): LowLevelSource[F, Map[String, KinesisMetadata[F]]] =
    new LowLevelSource[F, Map[String, KinesisMetadata[F]]] {
      def checkpointer: KinesisCheckpointer[F] = kinesisCheckpointer[F]

      def stream: Stream[F, Stream[F, LowLevelEvents[Map[String, KinesisMetadata[F]]]]] =
        Stream.emit(kinesisStream(config, livenessRef))

      def lastLiveness: F[FiniteDuration] =
        livenessRef.get
    }

  private implicit def metadataSemigroup[F[_]]: Semigroup[KinesisMetadata[F]] = new Semigroup[KinesisMetadata[F]] {
    override def combine(x: KinesisMetadata[F], y: KinesisMetadata[F]): KinesisMetadata[F] =
      if (x.sequenceNumber > y.sequenceNumber) x else y
  }

  private def kinesisCheckpointer[F[_]: Async]: KinesisCheckpointer[F] = new KinesisCheckpointer[F] {
    def combine(x: Map[String, KinesisMetadata[F]], y: Map[String, KinesisMetadata[F]]): Map[String, KinesisMetadata[F]] =
      x |+| y

    val empty: Map[String, KinesisMetadata[F]] = Map.empty
    def ack(c: Map[String, KinesisMetadata[F]]): F[Unit] =
      c.values.toList
        .parTraverse_ { metadata =>
          metadata.ack
            .recoverWith {
              case _: ShutdownException =>
                // The ShardRecordProcessor instance has been shutdown. This just means another KCL
                // worker has stolen our lease. It is expected during autoscaling of instances, and is
                // safe to ignore.
                Logger[F].warn(s"Skipping checkpointing of shard ${metadata.shardId} because this worker no longer owns the lease")

              case _: IllegalArgumentException if metadata.isLastInShard =>
                // See https://github.com/snowplow/enrich/issues/657
                // This can happen at the shard end when KCL no longer allows checkpointing of the last record in the shard.
                // We need to release the semaphore, so that fs2-aws handles checkpointing the end of the shard.
                Logger[F].warn(
                  s"Checkpointing failed on last record in shard. Ignoring error and instead try checkpointing of the shard end"
                ) *>
                  Sync[F].delay(metadata.lastRecordSemaphore.release())

              case _: IllegalArgumentException if metadata.lastRecordSemaphore.availablePermits === 0 =>
                // See https://github.com/snowplow/enrich/issues/657 and https://github.com/snowplow/enrich/pull/658
                // This can happen near the shard end, e.g. the penultimate batch in the shard, when KCL has already enqueued the final record in the shard to the fs2 queue.
                // We must not release the semaphore yet, because we are not ready for fs2-aws to checkpoint the end of the shard.
                // We can safely ignore the exception and move on.
                Logger[F].warn(
                  s"Checkpointing failed on a record which was not the last in the shard. Meanwhile, KCL has already enqueued the final record in the shard to the fs2 queue. Ignoring error and instead continue processing towards the shard end"
                )
            }
        }
    def nack(c: Map[String, KinesisMetadata[F]]): F[Unit] = Applicative[F].unit
  }

  private def kinesisStream[F[_]: Async](
    config: KinesisSourceConfig,
    livenessRef: Ref[F, FiniteDuration]
  ): Stream[F, LowLevelEvents[Map[String, KinesisMetadata[F]]]] =
    for {
      region <- Stream.eval(Sync[F].delay((new DefaultAwsRegionProviderChain).getRegion))
      consumerSettings = KinesisConsumerSettings(
                           config.streamName,
                           config.appName,
                           region,
                           bufferSize = config.bufferSize
                         )
      kinesisClient <- Stream.resource(mkKinesisClient[F](region, config.customEndpoint))
      dynamoClient <- Stream.resource(mkDynamoDbClient[F](region, config.dynamodbCustomEndpoint))
      cloudWatchClient <- Stream.resource(mkCloudWatchClient[F](region, config.cloudwatchCustomEndpoint))
      kinesis = Kinesis.create(scheduler(kinesisClient, dynamoClient, cloudWatchClient, config, _))
      chunk <- kinesis.readChunkedFromKinesisStream(consumerSettings)
      now <- Stream.eval(Sync[F].realTime)
      _ <- Stream.eval(livenessRef.set(now))
      if chunk.nonEmpty
    } yield {
      val ack = chunk.asSeq
        .groupBy(_.shardId)
        .map { case (k, records) =>
          k -> records.maxBy(_.sequenceNumber).toMetadata[F]
        }
        .toMap
      val earliestTstamp = chunk.iterator.map(_.record.approximateArrivalTimestamp).min
      LowLevelEvents(chunk.map(_.record.data()), ack, Some(earliestTstamp))
    }

  private def initialPositionOf(config: KinesisSourceConfig.InitialPosition): InitialPositionInStreamExtended =
    config match {
      case KinesisSourceConfig.InitialPosition.Latest => InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
      case KinesisSourceConfig.InitialPosition.TrimHorizon =>
        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
      case KinesisSourceConfig.InitialPosition.AtTimestamp(instant) =>
        InitialPositionInStreamExtended.newInitialPositionAtTimestamp(Date.from(instant))
    }

  private def scheduler[F[_]: Sync](
    kinesisClient: KinesisAsyncClient,
    dynamoDbClient: DynamoDbAsyncClient,
    cloudWatchClient: CloudWatchAsyncClient,
    kinesisConfig: KinesisSourceConfig,
    recordProcessorFactory: ShardRecordProcessorFactory
  ): F[Scheduler] =
    Sync[F].delay {
      val configsBuilder =
        new ConfigsBuilder(
          kinesisConfig.streamName,
          kinesisConfig.appName,
          kinesisClient,
          dynamoDbClient,
          cloudWatchClient,
          kinesisConfig.workerIdentifier,
          recordProcessorFactory
        )

      val retrievalConfig =
        configsBuilder.retrievalConfig
          .streamTracker(new SingleStreamTracker(kinesisConfig.streamName, initialPositionOf(kinesisConfig.initialPosition)))
          .retrievalSpecificConfig {
            kinesisConfig.retrievalMode match {
              case KinesisSourceConfig.Retrieval.FanOut =>
                new FanOutConfig(kinesisClient).streamName(kinesisConfig.streamName).applicationName(kinesisConfig.appName)
              case KinesisSourceConfig.Retrieval.Polling(maxRecords) =>
                new PollingConfig(kinesisConfig.streamName, kinesisClient).maxRecords(maxRecords)
            }
          }

      val leaseManagementConfig =
        configsBuilder.leaseManagementConfig
          .failoverTimeMillis(kinesisConfig.leaseDuration.toMillis)

      // We ask to see empty batches, so that we can update the health check even when there are no records in the stream
      val processorConfig =
        configsBuilder.processorConfig
          .callProcessRecordsEvenForEmptyRecordList(true)

      new Scheduler(
        configsBuilder.checkpointConfig,
        configsBuilder.coordinatorConfig,
        leaseManagementConfig,
        configsBuilder.lifecycleConfig,
        configsBuilder.metricsConfig.metricsLevel(MetricsLevel.NONE),
        processorConfig,
        retrievalConfig
      )
    }

  private def mkKinesisClient[F[_]: Sync](region: Region, customEndpoint: Option[URI]): Resource[F, KinesisAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        val builder =
          KinesisAsyncClient
            .builder()
            .region(region)
            .defaultsMode(DefaultsMode.AUTO)
        val customized = customEndpoint.map(builder.endpointOverride).getOrElse(builder)
        customized.build
      }
    }

  private def mkDynamoDbClient[F[_]: Sync](region: Region, customEndpoint: Option[URI]): Resource[F, DynamoDbAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        val builder =
          DynamoDbAsyncClient
            .builder()
            .region(region)
            .defaultsMode(DefaultsMode.AUTO)
        val customized = customEndpoint.map(builder.endpointOverride).getOrElse(builder)
        customized.build
      }
    }

  private def mkCloudWatchClient[F[_]: Sync](region: Region, customEndpoint: Option[URI]): Resource[F, CloudWatchAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        val builder =
          CloudWatchAsyncClient
            .builder()
            .region(region)
            .defaultsMode(DefaultsMode.AUTO)
        val customized = customEndpoint.map(builder.endpointOverride).getOrElse(builder)
        customized.build
      }
    }
}
