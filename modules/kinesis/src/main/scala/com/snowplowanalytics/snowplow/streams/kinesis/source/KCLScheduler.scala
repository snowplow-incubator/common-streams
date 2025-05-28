/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kinesis.source

import cats.effect.implicits._
import cats.effect.{Async, Resource, Sync}
import cats.implicits._
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.{ConfigsBuilder, InitialPositionInStream, InitialPositionInStreamExtended}
import software.amazon.kinesis.coordinator.{Scheduler, WorkerStateChangeListener}
import software.amazon.kinesis.metrics.MetricsLevel
import software.amazon.kinesis.processor.SingleStreamTracker
import software.amazon.kinesis.retrieval.fanout.FanOutConfig
import software.amazon.kinesis.retrieval.polling.PollingConfig

import java.net.URI
import java.util.Date
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.atomic.AtomicReference

import com.snowplowanalytics.snowplow.streams.kinesis.KinesisSourceConfig

private[source] object KCLScheduler {

  def populateQueue[F[_]: Async](
    config: KinesisSourceConfig,
    queue: SynchronousQueue[KCLAction],
    client: SdkAsyncHttpClient
  ): Resource[F, Unit] =
    for {
      kinesis <- mkKinesisClient[F](config.customEndpoint, client)
      dynamo <- mkDynamoDbClient[F](config.dynamodbCustomEndpoint, client)
      cloudWatch <- mkCloudWatchClient[F](config.cloudwatchCustomEndpoint, client)
      scheduler <- Resource.eval(mkScheduler(kinesis, dynamo, cloudWatch, config, queue))
      _ <- runInBackground(scheduler)
    } yield ()

  private def mkScheduler[F[_]: Sync](
    kinesisClient: KinesisAsyncClient,
    dynamoDbClient: DynamoDbAsyncClient,
    cloudWatchClient: CloudWatchAsyncClient,
    kinesisConfig: KinesisSourceConfig,
    queue: SynchronousQueue[KCLAction]
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
          () => ShardRecordProcessor(queue, new AtomicReference(Set.empty[String]))
        )

      val retrievalConfig =
        configsBuilder.retrievalConfig
          .streamTracker(new SingleStreamTracker(kinesisConfig.streamName, initialPositionOf(kinesisConfig.initialPosition)))
          .retrievalSpecificConfig {
            kinesisConfig.retrievalMode match {
              case KinesisSourceConfig.Retrieval.FanOut =>
                new FanOutConfig(kinesisClient).streamName(kinesisConfig.streamName).applicationName(kinesisConfig.appName)
              case KinesisSourceConfig.Retrieval.Polling(maxRecords) =>
                val c = new PollingConfig(kinesisConfig.streamName, kinesisClient).maxRecords(maxRecords)
                c.recordsFetcherFactory.maxPendingProcessRecordsInput(1)
                c
            }
          }

      val leaseManagementConfig =
        configsBuilder.leaseManagementConfig
          .failoverTimeMillis(kinesisConfig.leaseDuration.toMillis)
          .maxLeasesToStealAtOneTime(chooseMaxLeasesToStealAtOneTime(kinesisConfig))

      // We ask to see empty batches, so that we can update the health check even when there are no records in the stream
      val processorConfig =
        configsBuilder.processorConfig
          .callProcessRecordsEvenForEmptyRecordList(true)

      val coordinatorConfig = configsBuilder.coordinatorConfig
        .workerStateChangeListener(new WorkerStateChangeListener {
          def onWorkerStateChange(newState: WorkerStateChangeListener.WorkerState): Unit = ()
          override def onAllInitializationAttemptsFailed(e: Throwable): Unit =
            queue.put(KCLAction.KCLError(e))
        })

      new Scheduler(
        configsBuilder.checkpointConfig,
        coordinatorConfig,
        leaseManagementConfig,
        configsBuilder.lifecycleConfig,
        configsBuilder.metricsConfig.metricsLevel(MetricsLevel.NONE),
        processorConfig,
        retrievalConfig
      )
    }

  private def runInBackground[F[_]: Async](scheduler: Scheduler): Resource[F, Unit] =
    Sync[F].blocking(scheduler.run()).background *> Resource.onFinalize(Sync[F].blocking(scheduler.shutdown()))

  private def initialPositionOf(config: KinesisSourceConfig.InitialPosition): InitialPositionInStreamExtended =
    config match {
      case KinesisSourceConfig.InitialPosition.Latest => InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
      case KinesisSourceConfig.InitialPosition.TrimHorizon =>
        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
      case KinesisSourceConfig.InitialPosition.AtTimestamp(instant) =>
        InitialPositionInStreamExtended.newInitialPositionAtTimestamp(Date.from(instant))
    }

  private def mkKinesisClient[F[_]: Sync](customEndpoint: Option[URI], client: SdkAsyncHttpClient): Resource[F, KinesisAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].blocking { // Blocking because this might dial the EC2 metadata endpoint
        val builder = KinesisAsyncClient
          .builder()
          .defaultsMode(DefaultsMode.AUTO)
          .httpClient(client)
        customEndpoint.foreach(uri => builder.endpointOverride(uri))
        builder.build()
      }
    }

  private def mkDynamoDbClient[F[_]: Sync](customEndpoint: Option[URI], client: SdkAsyncHttpClient): Resource[F, DynamoDbAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].blocking { // Blocking because this might dial the EC2 metadata endpoint
        val builder = DynamoDbAsyncClient
          .builder()
          .defaultsMode(DefaultsMode.AUTO)
          .httpClient(client)
        customEndpoint.foreach(uri => builder.endpointOverride(uri))
        builder.build
      }
    }

  private def mkCloudWatchClient[F[_]: Sync](customEndpoint: Option[URI], client: SdkAsyncHttpClient): Resource[F, CloudWatchAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].blocking { // Blocking because this might dial the EC2 metadata endpoint
        val builder = CloudWatchAsyncClient
          .builder()
          .defaultsMode(DefaultsMode.AUTO)
          .httpClient(client)
        customEndpoint.foreach(uri => builder.endpointOverride(uri))
        builder.build
      }
    }

  /**
   * In order to avoid latency during scale up/down and pod-rotation, we want the app to be quick to
   * acquire shard-leases to process. With bigger instances (more cores) we tend to have more
   * shard-leases per instance, so we increase how aggressively it acquires leases.
   */
  private def chooseMaxLeasesToStealAtOneTime(config: KinesisSourceConfig): Int =
    (Runtime.getRuntime.availableProcessors * config.maxLeasesToStealAtOneTimeFactor)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt

}
