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
import org.apache.kafka.common.errors.{InvalidProducerEpochException, OutOfOrderSequenceException, TimeoutException => KafkaTimeoutException}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition, Uuid}
import org.specs2.Specification

import com.snowplowanalytics.snowplow.streams.{ListOfList, Sinkable}
import com.snowplowanalytics.snowplow.streams.kafka.sink.KafkaSink

import java.time.Duration
import java.util.concurrent.{CompletableFuture, Future => JFuture}
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

class KafkaSinkSpec extends Specification with CatsEffect {

  def is = s2"""
  KafkaSink should:
    Successfully send a batch to the producer $sendsBatch
    Replace the producer and retry after OutOfOrderSequenceException $recoversFromOutOfOrderSequence
    Replace the producer and retry after InvalidProducerEpochException $recoversFromInvalidProducerEpoch
    Replace the producer and retry after KafkaTimeoutException when idempotence is enabled $recoversFromTimeoutWithIdempotence
    Not retry KafkaTimeoutException when idempotence is disabled $doesNotRetryTimeoutWithoutIdempotence
    Not retry on an unrelated error $doesNotRetryOnUnrelatedError
  """

  private val config = KafkaSinkConfigM[Id](
    topicName        = "test-topic",
    bootstrapServers = "localhost:9092",
    producerConf     = Map.empty
  )

  private val idempotentConfig = config.copy(
    producerConf = Map("enable.idempotence" -> "true")
  )

  private def newMock(autoComplete: Boolean): MockProducer[String, Array[Byte]] =
    new MockProducer[String, Array[Byte]](autoComplete, new StringSerializer, new ByteArraySerializer)

  private def testBatch: ListOfList[Sinkable] =
    ListOfList.ofItems(Sinkable(bytes = "hello".getBytes, partitionKey = Some("k"), attributes = Map.empty))

  /** A Producer that either completes futures successfully or exceptionally, tracking send count */
  private def testProducer(
    failWith: Option[Exception],
    sendCount: AtomicInteger = new AtomicInteger(0)
  ): Producer[String, Array[Byte]] = new Producer[String, Array[Byte]] {
    def send(r: ProducerRecord[String, Array[Byte]]): JFuture[RecordMetadata] = {
      sendCount.incrementAndGet()
      val f = new CompletableFuture[RecordMetadata]()
      failWith match {
        case Some(e) => f.completeExceptionally(e)
        case None    => f.complete(new RecordMetadata(new TopicPartition(r.topic(), 0), 0L, 0, 0L, -1, -1))
      }
      f
    }
    def send(r: ProducerRecord[String, Array[Byte]], cb: Callback): JFuture[RecordMetadata] = send(r)
    def flush(): Unit = ()
    def partitionsFor(topic: String): java.util.List[PartitionInfo]                                  = java.util.Collections.emptyList()
    def metrics(): java.util.Map[MetricName, _ <: Metric]                                            = java.util.Collections.emptyMap()
    def close(): Unit                                                                                 = ()
    def close(timeout: Duration): Unit                                                               = ()
    def initTransactions(): Unit                                                                      = ()
    def beginTransaction(): Unit                                                                      = ()
    def commitTransaction(): Unit                                                                     = ()
    def abortTransaction(): Unit                                                                      = ()
    def clientInstanceId(timeout: Duration): Uuid                                                    = Uuid.ZERO_UUID
    def sendOffsetsToTransaction(o: java.util.Map[TopicPartition, OffsetAndMetadata], m: ConsumerGroupMetadata): Unit = ()
    def sendOffsetsToTransaction(o: java.util.Map[TopicPartition, OffsetAndMetadata], g: String): Unit               = ()
  }

  def sendsBatch = {
    val mock = newMock(true)
    KafkaSink.resourceWithFactory[IO](config, IO.pure(mock)).use { sink =>
      sink.sink(testBatch)
    } map { _ =>
      mock.history.asScala must haveSize(1)
    }
  }

  def recoversFromOutOfOrderSequence = {
    val failingSendCount  = new AtomicInteger(0)
    val successSendCount  = new AtomicInteger(0)
    val callCount         = new AtomicInteger(0)

    val makeProducer: IO[Producer[String, Array[Byte]]] = IO {
      if (callCount.getAndIncrement() == 0) testProducer(Some(new OutOfOrderSequenceException("test")), failingSendCount)
      else testProducer(None, successSendCount)
    }

    KafkaSink.resourceWithFactory[IO](config, makeProducer).use { sink =>
      sink.sink(testBatch)
    } map { _ =>
      // Initial send failed; producer was replaced and batch retried on the new producer
      (failingSendCount.get() must_== 1) and (successSendCount.get() must_== 1)
    }
  }

  def recoversFromInvalidProducerEpoch = {
    val failingSendCount  = new AtomicInteger(0)
    val successSendCount  = new AtomicInteger(0)
    val callCount         = new AtomicInteger(0)

    val makeProducer: IO[Producer[String, Array[Byte]]] = IO {
      if (callCount.getAndIncrement() == 0) testProducer(Some(new InvalidProducerEpochException("test")), failingSendCount)
      else testProducer(None, successSendCount)
    }

    KafkaSink.resourceWithFactory[IO](config, makeProducer).use { sink =>
      sink.sink(testBatch)
    } map { _ =>
      (failingSendCount.get() must_== 1) and (successSendCount.get() must_== 1)
    }
  }

  // KAFKA-7848: idempotent producers surface OUT_OF_ORDER_SEQUENCE_NUMBER as KafkaTimeoutException
  def recoversFromTimeoutWithIdempotence = {
    val failingSendCount = new AtomicInteger(0)
    val successSendCount = new AtomicInteger(0)
    val callCount        = new AtomicInteger(0)

    val makeProducer: IO[Producer[String, Array[Byte]]] = IO {
      if (callCount.getAndIncrement() == 0) testProducer(Some(new KafkaTimeoutException("delivery.timeout.ms expired")), failingSendCount)
      else testProducer(None, successSendCount)
    }

    KafkaSink.resourceWithFactory[IO](idempotentConfig, makeProducer).use { sink =>
      sink.sink(testBatch)
    } map { _ =>
      (failingSendCount.get() must_== 1) and (successSendCount.get() must_== 1)
    }
  }

  // Without idempotence, KafkaTimeoutException should NOT trigger producer replacement
  def doesNotRetryTimeoutWithoutIdempotence = {
    val callCount = new AtomicInteger(0)
    val makeProducer: IO[Producer[String, Array[Byte]]] = IO {
      callCount.incrementAndGet()
      testProducer(Some(new KafkaTimeoutException("delivery.timeout.ms expired")))
    }

    KafkaSink.resourceWithFactory[IO](config, makeProducer).use { sink =>
      sink.sink(testBatch).attempt
    } map { result =>
      (result must beLeft) and (callCount.get() must_== 1)
    }
  }

  def doesNotRetryOnUnrelatedError = {
    val callCount = new AtomicInteger(0)
    val makeProducer: IO[Producer[String, Array[Byte]]] = IO {
      callCount.incrementAndGet()
      testProducer(Some(new RuntimeException("unrelated error")))
    }

    KafkaSink.resourceWithFactory[IO](config, makeProducer).use { sink =>
      sink.sink(testBatch).attempt
    } map { result =>
      // Error must propagate and producer must NOT be replaced (callCount stays at 1)
      (result must beLeft) and (callCount.get() must_== 1)
    }
  }
}
