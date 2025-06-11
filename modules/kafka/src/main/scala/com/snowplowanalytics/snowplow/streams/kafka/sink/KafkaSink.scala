/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kafka.sink

import cats.implicits._
import cats.effect.{Async, Resource, Sync}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.streams.{ListOfList, Sink, Sinkable}
import com.snowplowanalytics.snowplow.streams.kafka.KafkaSinkConfig

import java.util.UUID
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

private[kafka] object KafkaSink {

  def resource[F[_]: Async](
    config: KafkaSinkConfig,
    authHandlerClass: String
  ): Resource[F, Sink[F]] =
    for {
      producer <- makeProducer(config, authHandlerClass)
      ec <- createExecutionContext
    } yield impl(config, producer, ec)

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  private def makeProducer[F[_]: Async](
    config: KafkaSinkConfig,
    authHandlerClass: String
  ): Resource[F, KafkaProducer[String, Array[Byte]]] = {
    val producerSettings = Map(
      "bootstrap.servers" -> config.bootstrapServers,
      "sasl.login.callback.handler.class" -> authHandlerClass,
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
    new Sink[F] {
      def sink(batch: ListOfList[Sinkable]): F[Unit] = {
        val f = Sync[F].delay {
          val futures = batch.asIterable.map { e =>
            val record = toProducerRecord(config, e)
            producer.send(record)
          }.toIndexedSeq

          futures.foreach(_.get)
        }
        Async[F].evalOn(f, ec)
      }

      def isHealthy: F[Boolean] =
        Logger[F].info(s"Checking whether topic ${config.topicName} has leaders for all partitions") >>
          Sync[F]
            .blocking {
              producer.partitionsFor(config.topicName).asScala
            }
            .flatMap { partitions =>
              if (partitions.isEmpty)
                Logger[F].warn(s"Topic ${config.topicName} has no partitions").as(false)
              else if (partitions.exists(p => Option(p.leader()).isEmpty))
                Logger[F].warn(s"Topic ${config.topicName} has partitions with no leader").as(false)
              else
                Logger[F].info(s"Confirmed topic ${config.topicName} has leaders for all partitions").as(true)
            }
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
