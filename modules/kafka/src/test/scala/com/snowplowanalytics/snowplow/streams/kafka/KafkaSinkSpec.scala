/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kafka

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import org.apache.kafka.clients.producer.{MockProducer, Producer}
import org.apache.kafka.common.errors.{InvalidProducerEpochException, OutOfOrderSequenceException}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.specs2.Specification

import com.snowplowanalytics.snowplow.streams.{ListOfList, Sinkable}
import com.snowplowanalytics.snowplow.streams.kafka.sink.KafkaSink

import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

class KafkaSinkSpec extends Specification with CatsEffect {

  def is = s2"""
  KafkaSink should:
    Successfully send a batch to the producer $sendsBatch
    Replace the producer and retry after OutOfOrderSequenceException $recoversFromOutOfOrderSequence
    Replace the producer and retry after InvalidProducerEpochException $recoversFromInvalidProducerEpoch
    Not retry on an unrelated ExecutionException $doesNotRetryOnUnrelatedError
  """

  private val config = KafkaSinkConfig(
    topicName        = "test-topic",
    bootstrapServers = "localhost:9092",
    producerConf     = Map.empty
  )

  private def newMock(autoComplete: Boolean): MockProducer[String, Array[Byte]] =
    new MockProducer[String, Array[Byte]](autoComplete, new StringSerializer, new ByteArraySerializer)

  private def testBatch: ListOfList[Sinkable] =
    ListOfList.ofItems(Sinkable(bytes = "hello".getBytes, partitionKey = Some("k"), attributes = Map.empty))

  def sendsBatch = {
    val mock = newMock(true)
    KafkaSink.resourceWithFactory[IO](config, IO.pure(mock)).use { sink =>
      sink.sink(testBatch)
    } map { _ =>
      mock.history.asScala must haveSize(1)
    }
  }

  def recoversFromOutOfOrderSequence = {
    val failingMock   = newMock(true)
    val recoveredMock = newMock(true)
    val callCount     = new AtomicInteger(0)

    failingMock.errorNext(new OutOfOrderSequenceException("test error"))

    val makeProducer: IO[Producer[String, Array[Byte]]] = IO {
      if (callCount.getAndIncrement() == 0) failingMock
      else recoveredMock
    }

    KafkaSink.resourceWithFactory[IO](config, makeProducer).use { sink =>
      sink.sink(testBatch)
    } map { _ =>
      failingMock.history.asScala must beEmpty and
        (recoveredMock.history.asScala must haveSize(1))
    }
  }

  def recoversFromInvalidProducerEpoch = {
    val failingMock   = newMock(true)
    val recoveredMock = newMock(true)
    val callCount     = new AtomicInteger(0)

    failingMock.errorNext(new InvalidProducerEpochException("test error"))

    val makeProducer: IO[Producer[String, Array[Byte]]] = IO {
      if (callCount.getAndIncrement() == 0) failingMock
      else recoveredMock
    }

    KafkaSink.resourceWithFactory[IO](config, makeProducer).use { sink =>
      sink.sink(testBatch)
    } map { _ =>
      failingMock.history.asScala must beEmpty and
        (recoveredMock.history.asScala must haveSize(1))
    }
  }

  def doesNotRetryOnUnrelatedError = {
    val failingMock = newMock(true)
    failingMock.errorNext(new RuntimeException("unrelated error"))

    KafkaSink.resourceWithFactory[IO](config, IO.pure(failingMock)).use { sink =>
      sink.sink(testBatch).attempt
    } map { result =>
      result must beLeft
    }
  }
}
