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
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.specs2.Specification

import com.snowplowanalytics.snowplow.streams.{ListOfList, Sinkable}
import com.snowplowanalytics.snowplow.streams.kafka.sink.KafkaSink

import scala.jdk.CollectionConverters._

class KafkaSinkSpec extends Specification with CatsEffect {

  def is = s2"""
  KafkaSink should:
    Successfully send a batch using the blocking wait path $sendsBatch
    Send multiple events in a single batch $sendsMultipleEvents
    Propagate an error from future.get() through the blocking path $propagatesError
  """

  private val config = KafkaSinkConfig(
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
    val mock = newMock(true)
    mock.errorNext(new RuntimeException("broker error"))

    KafkaSink.resourceWithProducer[IO](config, mock).use { sink =>
      sink.sink(batch(1)).attempt
    } map { result =>
      result must beLeft
    }
  }
}
