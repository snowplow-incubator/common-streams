/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks.kafka

import cats.effect.{Async, Resource, Sync}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import com.snowplowanalytics.snowplow.sinks.{Sink, Sinkable}
import com.snowplowanalytics.snowplow.azure.AzureAuthenticationCallbackHandler

import java.util.UUID
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.jdk.CollectionConverters._

object KafkaSink {

  def resource[F[_]: Async, T <: AzureAuthenticationCallbackHandler](
    config: KafkaSinkConfig,
    authHandlerClass: ClassTag[T]
  ): Resource[F, Sink[F]] =
    for {
      producer <- makeProducer(config, authHandlerClass)
      ec <- createExecutionContext
    } yield impl(config, producer, ec)

  private def makeProducer[F[_]: Async, T <: AzureAuthenticationCallbackHandler](
    config: KafkaSinkConfig,
    authHandlerClass: ClassTag[T]
  ): Resource[F, KafkaProducer[String, Array[Byte]]] = {
    val producerSettings = Map(
      "bootstrap.servers" -> config.bootstrapServers,
      "sasl.login.callback.handler.class" -> authHandlerClass.runtimeClass.getName,
      "key.serializer" -> classOf[StringSerializer].getName,
      "value.serializer" -> classOf[ByteArraySerializer].getName
    ) ++ config.producerConf
    val make = Sync[F].delay {
      new KafkaProducer[String, Array[Byte]]((producerSettings: Map[String, AnyRef]).asJava)
    }
    Resource.make(make)(p => Sync[F].blocking(p.close))
  }

  private def impl[F[_]: Async](
    config: KafkaSinkConfig,
    producer: KafkaProducer[String, Array[Byte]],
    ec: ExecutionContext
  ): Sink[F] =
    Sink { batch =>
      val f = Sync[F].delay {
        val futures = batch.asIterable.map { e =>
          val record = toProducerRecord(config, e)
          producer.send(record)
        }.toIndexedSeq

        futures.foreach(_.get)
      }
      Async[F].evalOn(f, ec)
    }

  private def toProducerRecord(config: KafkaSinkConfig, sinkable: Sinkable): ProducerRecord[String, Array[Byte]] = {

    val headers = sinkable.attributes.map { case (k, v) =>
      new Header {
        def key: String        = k
        def value: Array[Byte] = v.getBytes(StandardCharsets.UTF_8)
      }
    }

    new ProducerRecord(config.topicName, null, sinkable.partitionKey.getOrElse(UUID.randomUUID.toString), sinkable.bytes, headers.asJava)
  }

  private def createExecutionContext[F[_]: Sync]: Resource[F, ExecutionContext] = {
    val make = Sync[F].delay {
      Executors.newSingleThreadExecutor
    }
    Resource.make(make)(e => Sync[F].blocking(e.shutdown)).map(ExecutionContext.fromExecutorService(_))
  }
}
