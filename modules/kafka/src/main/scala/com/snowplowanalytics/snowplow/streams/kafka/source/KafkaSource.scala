/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kafka.source

import cats.Applicative
import cats.effect.{Async, Resource, Sync}
import cats.implicits._
import cats.kernel.Semigroup
import fs2.Stream
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.ByteBuffer
import scala.concurrent.duration.{DurationLong, FiniteDuration}

// kafka
import fs2.kafka._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.errors.RebalanceInProgressException

// snowplow
import com.snowplowanalytics.snowplow.streams.SourceAndAck
import com.snowplowanalytics.snowplow.streams.internal.{Checkpointer, LowLevelEvents, LowLevelSource}
import com.snowplowanalytics.snowplow.streams.kafka.KafkaSourceConfig

private[kafka] object KafkaSource {

  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def build[F[_]: Async](
    config: KafkaSourceConfig,
    authHandlerClass: String
  ): Resource[F, SourceAndAck[F]] =
    for {
      kafkaConsumer <- KafkaConsumer.resource(consumerSettings[F](config, authHandlerClass))
      result <- Resource.eval(LowLevelSource.toSourceAndAck(lowLevel(config, kafkaConsumer)))
    } yield result

  private def lowLevel[F[_]: Async](
    config: KafkaSourceConfig,
    kafkaConsumer: KafkaConsumer[F, Array[Byte], ByteBuffer]
  ): LowLevelSource[F, KafkaCheckpoints] =
    new LowLevelSource[F, KafkaCheckpoints] {
      def checkpointer: Checkpointer[F, KafkaCheckpoints] = kafkaCheckpointer(kafkaConsumer)

      def stream: Stream[F, Stream[F, Option[LowLevelEvents[KafkaCheckpoints]]]] =
        kafkaStream(config, kafkaConsumer)

      def debounceCheckpoints: FiniteDuration = config.debounceCommitOffsets
    }

  type KafkaCheckpoints = Map[TopicPartition, OffsetAndMetadata]

  private implicit def offsetAndMetadataSemigroup: Semigroup[OffsetAndMetadata] = new Semigroup[OffsetAndMetadata] {
    def combine(x: OffsetAndMetadata, y: OffsetAndMetadata): OffsetAndMetadata =
      if (x.offset > y.offset) x else y
  }

  private def kafkaCheckpointer[F[_]: Async](kafkaConsumer: KafkaConsumer[F, Array[Byte], ByteBuffer]): Checkpointer[F, KafkaCheckpoints] =
    new Checkpointer[F, KafkaCheckpoints] {
      def combine(x: KafkaCheckpoints, y: KafkaCheckpoints): KafkaCheckpoints =
        x |+| y

      val empty: KafkaCheckpoints = Map.empty
      def ack(c: KafkaCheckpoints): F[Unit] =
        kafkaConsumer.commitSync(c).recoverWith { case _: RebalanceInProgressException =>
          Logger[F].warn("Failed to commit offsets during rebalance, offsets will be lost and events may be reprocessed")
        }
      def nack(c: KafkaCheckpoints): F[Unit] = Applicative[F].unit
    }

  private def kafkaStream[F[_]: Async](
    config: KafkaSourceConfig,
    kafkaConsumer: KafkaConsumer[F, Array[Byte], ByteBuffer]
  ): Stream[F, Stream[F, Option[LowLevelEvents[KafkaCheckpoints]]]] =
    Stream.eval(kafkaConsumer.subscribeTo(config.topicName)) >>
      kafkaConsumer.partitionsMapStream
        .evalMapFilter(logWhenNoPartitions[F])
        .map(joinPartitions[F](_))

  private type PartitionedStreams[F[_]] = Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, Array[Byte], ByteBuffer]]]

  private def joinPartitions[F[_]: Async](
    partitioned: PartitionedStreams[F]
  ): Stream[F, Option[LowLevelEvents[KafkaCheckpoints]]] = {
    val streams = partitioned.toList.map { case (topicPartition, stream) =>
      stream.chunks
        .flatMap { chunk =>
          chunk.last match {
            case Some(last) =>
              val events = chunk.map {
                _.record.value
              }
              val ack = Map(topicPartition -> last.offset.offsetAndMetadata)
              val timestamps = chunk.iterator.flatMap { ccr =>
                val ts = ccr.record.timestamp
                ts.logAppendTime.orElse(ts.createTime).orElse(ts.unknownTime)
              }
              val earliestTimestamp = if (timestamps.isEmpty) None else Some(timestamps.min.millis)
              Stream.emit(Some(LowLevelEvents(events, ack, earliestTimestamp)))
            case None =>
              Stream.empty
          }
        }
    }

    val formatted = formatForLog(partitioned.keys)

    Stream.eval(Logger[F].info(s"Processsing partitions: $formatted")).drain ++
      streams.parJoinUnbounded
        .mergeHaltL(Stream.awakeDelay(10.seconds).map(_ => None).repeat) // keepalives
        .onFinalize {
          Logger[F].info(s"Stopping processing of partitions: $formatted")
        }
  }

  private def logWhenNoPartitions[F[_]: Sync](partitioned: PartitionedStreams[F]): F[Option[PartitionedStreams[F]]] =
    if (partitioned.isEmpty)
      Logger[F].info("No partitions are currently assigned to this processor").as(None)
    else
      Sync[F].pure(Some(partitioned))

  def formatForLog(tps: Iterable[TopicPartition]): String =
    tps
      .map { tp =>
        s"${tp.topic}-${tp.partition}"
      }
      .toSeq
      .sorted
      .mkString(",")

  private implicit def byteBufferDeserializer[F[_]: Sync]: Resource[F, ValueDeserializer[F, ByteBuffer]] =
    Resource.pure(Deserializer.lift(arr => Sync[F].pure(ByteBuffer.wrap(arr))))

  private def consumerSettings[F[_]: Async](
    config: KafkaSourceConfig,
    authHandlerClass: String
  ): ConsumerSettings[F, Array[Byte], ByteBuffer] =
    ConsumerSettings[F, Array[Byte], ByteBuffer]
      .withProperty("sasl.login.callback.handler.class", authHandlerClass)
      .withBootstrapServers(config.bootstrapServers)
      .withProperties(config.consumerConf)
      .withEnableAutoCommit(false)
      .withCommitTimeout(config.commitTimeout)
      .withRebalanceRevokeMode(RebalanceRevokeMode.Graceful)
}
