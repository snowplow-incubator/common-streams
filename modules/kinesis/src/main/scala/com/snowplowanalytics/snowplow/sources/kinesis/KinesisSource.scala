/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import cats.{Order, Semigroup}
import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.implicits._
import cats.implicits._
import fs2.{Chunk, Pull, Stream}
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.{ConfigsBuilder, InitialPositionInStream, InitialPositionInStreamExtended}
import software.amazon.kinesis.coordinator.{Scheduler, WorkerStateChangeListener}
import software.amazon.kinesis.exceptions.ShutdownException
import software.amazon.kinesis.lifecycle.events.{
  InitializationInput,
  LeaseLostInput,
  ProcessRecordsInput,
  ShardEndedInput,
  ShutdownRequestedInput
}
import software.amazon.kinesis.metrics.MetricsLevel
import software.amazon.kinesis.processor.{
  RecordProcessorCheckpointer,
  ShardRecordProcessor,
  ShardRecordProcessorFactory,
  SingleStreamTracker
}
import software.amazon.kinesis.retrieval.fanout.FanOutConfig
import software.amazon.kinesis.retrieval.polling.PollingConfig
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import java.net.URI
import java.util.Date
import java.util.concurrent.{CountDownLatch, SynchronousQueue}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import com.snowplowanalytics.snowplow.sources.internal.{Checkpointer, LowLevelEvents, LowLevelSource}
import com.snowplowanalytics.snowplow.sources.SourceAndAck

object KinesisSource {

  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def build[F[_]: Async](config: KinesisSourceConfig): F[SourceAndAck[F]] =
    Ref.ofEffect(Sync[F].realTime).flatMap { livenessRef =>
      LowLevelSource.toSourceAndAck(lowLevel(config, livenessRef))
    }

  sealed trait Checkpointable {
    def extendedSequenceNumber: ExtendedSequenceNumber
  }
  private case class RecordCheckpointable(extendedSequenceNumber: ExtendedSequenceNumber, checkpointer: RecordProcessorCheckpointer)
      extends Checkpointable
  private case class ShardEndCheckpointable(checkpointer: RecordProcessorCheckpointer, release: CountDownLatch) extends Checkpointable {
    override def extendedSequenceNumber: ExtendedSequenceNumber = ExtendedSequenceNumber.SHARD_END
  }

  private type KinesisCheckpointer[F[_]] = Checkpointer[F, Map[String, Checkpointable]]

  private def lowLevel[F[_]: Async](
    config: KinesisSourceConfig,
    livenessRef: Ref[F, FiniteDuration]
  ): LowLevelSource[F, Map[String, Checkpointable]] =
    new LowLevelSource[F, Map[String, Checkpointable]] {
      def checkpointer: KinesisCheckpointer[F] = kinesisCheckpointer[F]

      def stream: Stream[F, Stream[F, LowLevelEvents[Map[String, Checkpointable]]]] =
        kinesisStream(config, livenessRef)

      def lastLiveness: F[FiniteDuration] =
        livenessRef.get
    }

  private implicit def checkpointableOrder: Order[Checkpointable] = Order.from { case (a, b) =>
    a.extendedSequenceNumber.compareTo(b.extendedSequenceNumber)
  }

  private implicit def checkpointableSemigroup: Semigroup[Checkpointable] = new Semigroup[Checkpointable] {
    def combine(x: Checkpointable, y: Checkpointable): Checkpointable =
      x.max(y)
  }

  private def ignoreShutdownExceptions[F[_]: Sync](shardId: String): PartialFunction[Throwable, F[Unit]] = { case _: ShutdownException =>
    // The ShardRecordProcessor instance has been shutdown. This just means another KCL
    // worker has stolen our lease. It is expected during autoscaling of instances, and is
    // safe to ignore.
    Logger[F].warn(s"Skipping checkpointing of shard $shardId because this worker no longer owns the lease")
  }

  private def kinesisCheckpointer[F[_]: Async]: KinesisCheckpointer[F] = new KinesisCheckpointer[F] {
    def combine(x: Map[String, Checkpointable], y: Map[String, Checkpointable]): Map[String, Checkpointable] =
      x |+| y

    val empty: Map[String, Checkpointable] = Map.empty

    def ack(c: Map[String, Checkpointable]): F[Unit] =
      c.toList.parTraverse_ {
        case (shardId, RecordCheckpointable(extendedSequenceNumber, checkpointer)) =>
          Logger[F].debug(s"Checkpointing shard $shardId at $extendedSequenceNumber") *>
            Sync[F]
              .blocking(
                checkpointer.checkpoint(extendedSequenceNumber.sequenceNumber, extendedSequenceNumber.subSequenceNumber)
              )
              .recoverWith(ignoreShutdownExceptions(shardId))
        case (shardId, ShardEndCheckpointable(checkpointer, release)) =>
          Logger[F].debug(s"Checkpointing shard $shardId at SHARD_END") *>
            Sync[F].blocking(checkpointer.checkpoint()).recoverWith(ignoreShutdownExceptions(shardId)) *>
            Sync[F].delay(release.countDown)
      }

    def nack(c: Map[String, Checkpointable]): F[Unit] =
      Sync[F].unit
  }

  private sealed trait KCLAction
  private case class ProcessRecords(shardId: String, processRecordsInput: ProcessRecordsInput) extends KCLAction
  private case class ShardEnd(
    shardId: String,
    await: CountDownLatch,
    shardEndedInput: ShardEndedInput
  ) extends KCLAction
  private case class KCLError(t: Throwable) extends KCLAction

  private def kinesisStream[F[_]: Async](
    config: KinesisSourceConfig,
    livenessRef: Ref[F, FiniteDuration]
  ): Stream[F, Stream[F, LowLevelEvents[Map[String, Checkpointable]]]] =
    for {
      kinesisClient <- Stream.resource(mkKinesisClient[F](config.customEndpoint))
      dynamoClient <- Stream.resource(mkDynamoDbClient[F](config.dynamodbCustomEndpoint))
      cloudWatchClient <- Stream.resource(mkCloudWatchClient[F](config.cloudwatchCustomEndpoint))
      queue = new SynchronousQueue[KCLAction]
      scheduler <- Stream.eval(scheduler(kinesisClient, dynamoClient, cloudWatchClient, config, queue))
      _ <- Stream.resource(runRecordProcessor[F](scheduler))
      s <- Stream.emit(pullFromQueue(queue, livenessRef).stream).repeat
    } yield s

  private def pullFromQueue[F[_]: Sync](
    queue: SynchronousQueue[KCLAction],
    livenessRef: Ref[F, FiniteDuration]
  ): Pull[F, LowLevelEvents[Map[String, Checkpointable]], Unit] =
    for {
      maybeE <- Pull.eval(Sync[F].delay(Option(queue.poll)))
      e <- maybeE match {
             case Some(e) => Pull.pure(e)
             case None    => Pull.eval(Sync[F].interruptible(queue.take))
           }
      now <- Pull.eval(Sync[F].realTime)
      _ <- Pull.eval(livenessRef.set(now))
      _ <- e match {
             case ProcessRecords(_, processRecordsInput) if processRecordsInput.records.asScala.isEmpty =>
               pullFromQueue[F](queue, livenessRef)
             case ProcessRecords(shardId, processRecordsInput) =>
               val chunk      = Chunk.javaList(processRecordsInput.records()).map(_.data())
               val lastRecord = processRecordsInput.records.asScala.last // last is safe because we handled the empty case above
               val checkpointable = RecordCheckpointable(
                 new ExtendedSequenceNumber(lastRecord.sequenceNumber, lastRecord.subSequenceNumber),
                 processRecordsInput.checkpointer
               )
               val next =
                 LowLevelEvents(chunk, Map[String, Checkpointable](shardId -> checkpointable), Some(lastRecord.approximateArrivalTimestamp))
               Pull.output1(next).covary[F] *> pullFromQueue[F](queue, livenessRef)
             case ShardEnd(shardId, await, shardEndedInput) =>
               val checkpointable = ShardEndCheckpointable(shardEndedInput.checkpointer, await)
               val last           = LowLevelEvents(Chunk.empty, Map[String, Checkpointable](shardId -> checkpointable), None)
               Pull
                 .eval(Logger[F].info(s"Ending this window of events early because reached the end of Kinesis shard $shardId"))
                 .covaryOutput *>
                 Pull.output1(last).covary[F] *> Pull.done
             case KCLError(t) => Pull.raiseError[F](t)
           }
    } yield ()

  private def runRecordProcessor[F[_]: Async](scheduler: Scheduler): Resource[F, Unit] =
    Sync[F].blocking(scheduler.run()).background *> Resource.onFinalize(Sync[F].blocking(scheduler.shutdown()))

  private def shardRecordProcessor(queue: SynchronousQueue[KCLAction]): ShardRecordProcessor = new ShardRecordProcessor {
    private var shardId: String = _

    def initialize(initializationInput: InitializationInput): Unit =
      shardId = initializationInput.shardId

    def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
      val countDownLatch = new CountDownLatch(1)
      queue.put(ShardEnd(shardId, countDownLatch, shardEndedInput))
      countDownLatch.await()
    }

    def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
      val action = ProcessRecords(shardId, processRecordsInput)
      queue.put(action)
    }

    def leaseLost(leaseLostInput: LeaseLostInput): Unit = ()

    def shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput): Unit = ()
  }

  private def recordProcessorFactory(queue: SynchronousQueue[KCLAction]): ShardRecordProcessorFactory = { () =>
    shardRecordProcessor(queue)
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
          recordProcessorFactory(queue)
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

      val coordinatorConfig = configsBuilder.coordinatorConfig
        .workerStateChangeListener(new WorkerStateChangeListener {
          def onWorkerStateChange(newState: WorkerStateChangeListener.WorkerState): Unit = ()
          override def onAllInitializationAttemptsFailed(e: Throwable): Unit =
            queue.put(KCLError(e))
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

  private def mkKinesisClient[F[_]: Sync](customEndpoint: Option[URI]): Resource[F, KinesisAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].blocking { // Blocking because this might dial the EC2 metadata endpoint
        val builder =
          KinesisAsyncClient
            .builder()
            .defaultsMode(DefaultsMode.AUTO)
        val customized = customEndpoint.map(builder.endpointOverride).getOrElse(builder)
        customized.build
      }
    }

  private def mkDynamoDbClient[F[_]: Sync](customEndpoint: Option[URI]): Resource[F, DynamoDbAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].blocking { // Blocking because this might dial the EC2 metadata endpoint
        val builder =
          DynamoDbAsyncClient
            .builder()
            .defaultsMode(DefaultsMode.AUTO)
        val customized = customEndpoint.map(builder.endpointOverride).getOrElse(builder)
        customized.build
      }
    }

  private def mkCloudWatchClient[F[_]: Sync](customEndpoint: Option[URI]): Resource[F, CloudWatchAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].blocking { // Blocking because this might dial the EC2 metadata endpoint
        val builder =
          CloudWatchAsyncClient
            .builder()
            .defaultsMode(DefaultsMode.AUTO)
        val customized = customEndpoint.map(builder.endpointOverride).getOrElse(builder)
        customized.build
      }
    }
}
