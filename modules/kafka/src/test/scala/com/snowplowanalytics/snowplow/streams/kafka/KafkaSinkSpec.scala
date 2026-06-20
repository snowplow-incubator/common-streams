/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kafka

import cats.Id
import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import org.apache.kafka.clients.consumer.{ConsumerGroupMetadata, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{Callback, MockProducer, Producer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition, Uuid}
import org.specs2.Specification

import com.snowplowanalytics.snowplow.streams.{ListOfList, Sinkable}
import com.snowplowanalytics.snowplow.streams.kafka.sink.KafkaSink

import java.time.Duration
import java.util.concurrent.{CompletableFuture, Future => JFuture}
import scala.jdk.CollectionConverters._

class KafkaSinkSpec extends Specification with CatsEffect {

  def is = s2"""
  KafkaSink should:
    Successfully send a batch using the blocking wait path $sendsBatch
    Send multiple events in a single batch $sendsMultipleEvents
    Propagate an error from future.get() through the blocking path $propagatesError
  """

  private val config = KafkaSinkConfigM[Id](
    topicName        = "test-topic",
    bootstrapServers = "localhost:9092",
    producerConf     = Map.empty
  )

  private def newMock(autoComplete: Boolean): MockProducer[String, Array[Byte]] =
    new MockProducer[String, Array[Byte]](autoComplete, new StringSerializer, new ByteArraySerializer)

  private def batch(n: Int): ListOfList[Sinkable] =
    ListOfList.ofItems(
      (1 to n).map(i => Sinkable(bytes = s"event-$i".getBytes, partitionKey = Some(s"k$i"), attributes = Map.empty)): _*
    )

  /** A Producer backed by CompletableFutures, either completing successfully or exceptionally */
  private def testProducer(failWith: Option[Exception]): Producer[String, Array[Byte]] =
    new Producer[String, Array[Byte]] {
      def send(r: ProducerRecord[String, Array[Byte]]): JFuture[RecordMetadata] = {
        val f = new CompletableFuture[RecordMetadata]()
        failWith match {
          case Some(e) => f.completeExceptionally(e)
          case None    => f.complete(new RecordMetadata(new TopicPartition(r.topic(), 0), 0L, 0, 0L, -1, -1))
        }
        f
      }
      def send(r: ProducerRecord[String, Array[Byte]], cb: Callback): JFuture[RecordMetadata] = send(r)
      def flush(): Unit                                                                                              = ()
      def partitionsFor(topic: String): java.util.List[PartitionInfo]                                              = java.util.Collections.emptyList()
      def metrics(): java.util.Map[MetricName, _ <: Metric]                                                        = java.util.Collections.emptyMap()
      def close(): Unit                                                                                             = ()
      def close(timeout: Duration): Unit                                                                           = ()
      def initTransactions(): Unit                                                                                   = ()
      def beginTransaction(): Unit                                                                                   = ()
      def commitTransaction(): Unit                                                                                  = ()
      def abortTransaction(): Unit                                                                                   = ()
      def clientInstanceId(timeout: Duration): Uuid                                                                = Uuid.ZERO_UUID
      def sendOffsetsToTransaction(o: java.util.Map[TopicPartition, OffsetAndMetadata], m: ConsumerGroupMetadata): Unit = ()
      def sendOffsetsToTransaction(o: java.util.Map[TopicPartition, OffsetAndMetadata], g: String): Unit               = ()
    }

  def sendsBatch = {
    val mock = newMock(true)
    KafkaSink.resourceWithProducer[IO](config, mock).use { sink =>
      sink.sink(batch(1))
    } map { _ =>
      mock.history.asScala must haveSize(1)
    }
  }

  def sendsMultipleEvents = {
    val mock = newMock(true)
    KafkaSink.resourceWithProducer[IO](config, mock).use { sink =>
      sink.sink(batch(3))
    } map { _ =>
      mock.history.asScala must haveSize(3)
    }
  }

  def propagatesError = {
    KafkaSink.resourceWithProducer[IO](config, testProducer(Some(new RuntimeException("broker error")))).use { sink =>
      sink.sink(batch(1)).attempt
    } map { result =>
      result must beLeft
    }
  }
}
