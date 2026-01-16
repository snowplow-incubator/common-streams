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
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.retries.api.RetryStrategy
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.{ConfigsBuilder, InitialPositionInStream, InitialPositionInStreamExtended}
import software.amazon.kinesis.coordinator.{Scheduler, WorkerStateChangeListener}
import software.amazon.kinesis.metrics.NullMetricsFactory
import software.amazon.kinesis.processor.SingleStreamTracker
import software.amazon.kinesis.retrieval.fanout.FanOutConfig
import software.amazon.kinesis.retrieval.polling.PollingConfig

import java.net.URI
import java.util.Date
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicReference
import java.time.{Duration => JavaDuration}
import scala.concurrent.duration.FiniteDuration

import com.snowplowanalytics.snowplow.streams.kinesis.{AWS_USER_AGENT, KinesisSourceConfig}

private[source] object KCLScheduler {

  def populateQueue[F[_]: Async](
    config: KinesisSourceConfig,
    queue: LinkedBlockingQueue[KCLAction],
    client: SdkAsyncHttpClient
  ): Resource[F, Unit] = {
    val retryStrategy = AwsRetryStrategy
      .standardRetryStrategy()
      .toBuilder
      .maxAttempts(config.maxRetries)
      .build()
    for {
      kinesis <- mkKinesisClient[F](config.customEndpoint, config.apiCallAttemptTimeout, retryStrategy, client)
      dynamo <- mkDynamoDbClient[F](config.dynamodbCustomEndpoint, config.apiCallAttemptTimeout, retryStrategy, client)
      cloudWatch <- mkCloudWatchClient[F](config.cloudwatchCustomEndpoint, config.apiCallAttemptTimeout, retryStrategy, client)
      scheduler <- Resource.eval(mkScheduler(kinesis, dynamo, cloudWatch, config, queue))
      _ <- runInBackground(scheduler)
    } yield ()
  }

  private def mkScheduler[F[_]: Sync](
    kinesisClient: KinesisAsyncClient,
    dynamoDbClient: DynamoDbAsyncClient,
    cloudWatchClient: CloudWatchAsyncClient,
    kinesisConfig: KinesisSourceConfig,
    queue: LinkedBlockingQueue[KCLAction]
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
              case KinesisSourceConfig.Retrieval.Polling(maxRecords, idleTimeBetweenReads) =>
                val c = new PollingConfig(kinesisConfig.streamName, kinesisClient)
                  .maxRecords(maxRecords)
                  .idleTimeBetweenReadsInMillis(idleTimeBetweenReads.toMillis)
                c.recordsFetcherFactory.maxPendingProcessRecordsInput(1)
                c.kinesisRequestTimeout(
                  JavaDuration.ofMillis(Long.MaxValue)
                ) // Timeout is bounded by retry count * api call attempt timeout
                c
            }
          }

      val leaseManagementConfig =
        configsBuilder.leaseManagementConfig
          .failoverTimeMillis(kinesisConfig.leaseDuration.toMillis)
          .maxLeasesToStealAtOneTime(chooseMaxLeasesToStealAtOneTime(kinesisConfig))
          .dynamoDbRequestTimeout(JavaDuration.ofMillis(Long.MaxValue)) // Timeout is bounded by retry count * api call attempt timeout

      // We ask to see empty batches, so that we can update the health check even when there are no records in the stream
      val processorConfig =
        configsBuilder.processorConfig
          .callProcessRecordsEvenForEmptyRecordList(true)

      val coordinatorConfig = configsBuilder.coordinatorConfig
        .workerStateChangeListener(new WorkerStateChangeListener {
          def onWorkerStateChange(newState: WorkerStateChangeListener.WorkerState): Unit = ()
          override def onAllInitializationAttemptsFailed(e: Throwable): Unit = {
            val countDownLatch = new CountDownLatch(1)
            queue.put(KCLAction.KCLError(e, countDownLatch))
            countDownLatch.await()
            ()
          }
        })

      new Scheduler(
        configsBuilder.checkpointConfig,
        coordinatorConfig,
        leaseManagementConfig,
        configsBuilder.lifecycleConfig,
        configsBuilder.metricsConfig.metricsFactory(new NullMetricsFactory),
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

  private def mkKinesisClient[F[_]: Sync](
    customEndpoint: Option[URI],
    apiCallAttemptTimeout: FiniteDuration,
    retryStrategy: RetryStrategy,
    client: SdkAsyncHttpClient
  ): Resource[F, KinesisAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].blocking { // Blocking because this might dial the EC2 metadata endpoint
        val builder = KinesisAsyncClient
          .builder()
          .defaultsMode(DefaultsMode.AUTO)
          .httpClient(client)
          .overrideConfiguration { c =>
            c.retryStrategy(retryStrategy)
            c.apiCallAttemptTimeout(JavaDuration.ofMillis(apiCallAttemptTimeout.toMillis))
            c.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, AWS_USER_AGENT)
            ()
          }
        customEndpoint.foreach(uri => builder.endpointOverride(uri))
        builder.build()
      }
    }

  private def mkDynamoDbClient[F[_]: Sync](
    customEndpoint: Option[URI],
    apiCallAttemptTimeout: FiniteDuration,
    retryStrategy: RetryStrategy,
    client: SdkAsyncHttpClient
  ): Resource[F, DynamoDbAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].blocking { // Blocking because this might dial the EC2 metadata endpoint
        val builder = DynamoDbAsyncClient
          .builder()
          .defaultsMode(DefaultsMode.AUTO)
          .httpClient(client)
          .overrideConfiguration { c =>
            c.retryStrategy(retryStrategy)
            c.apiCallAttemptTimeout(JavaDuration.ofMillis(apiCallAttemptTimeout.toMillis))
            c.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, AWS_USER_AGENT)
            ()
          }
        customEndpoint.foreach(uri => builder.endpointOverride(uri))
        builder.build
      }
    }

  private def mkCloudWatchClient[F[_]: Sync](
    customEndpoint: Option[URI],
    apiCallAttemptTimeout: FiniteDuration,
    retryStrategy: RetryStrategy,
    client: SdkAsyncHttpClient
  ): Resource[F, CloudWatchAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].blocking { // Blocking because this might dial the EC2 metadata endpoint
        val builder = CloudWatchAsyncClient
          .builder()
          .defaultsMode(DefaultsMode.AUTO)
          .httpClient(client)
          .overrideConfiguration { c =>
            c.retryStrategy(retryStrategy)
            c.apiCallAttemptTimeout(JavaDuration.ofMillis(apiCallAttemptTimeout.toMillis))
            c.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, AWS_USER_AGENT)
            ()
          }
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
