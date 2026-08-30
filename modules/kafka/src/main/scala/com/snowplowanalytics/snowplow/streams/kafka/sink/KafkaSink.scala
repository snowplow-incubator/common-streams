/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kafka.sink

import cats.implicits._
import cats.effect.{Async, Ref, Resource, Sync}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.common.errors.{InvalidProducerEpochException, OutOfOrderSequenceException, TimeoutException => KafkaTimeoutException}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.streams.{ListOfList, Sink, Sinkable}
import com.snowplowanalytics.snowplow.streams.kafka.KafkaSinkConfig

import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.{ExecutionException, Executors}
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

private[kafka] object KafkaSink {

  def resource[F[_]: Async](
    config: KafkaSinkConfig,
    authHandlerClass: String
  ): Resource[F, Sink[F]] =
    resourceWithFactory(config, newProducer[F](config, authHandlerClass))

  // Visible to tests so they can inject a mock producer factory
  private[kafka] def resourceWithFactory[F[_]: Async](
    config: KafkaSinkConfig,
    makeProducer: F[Producer[String, Array[Byte]]]
  ): Resource[F, Sink[F]] = {
    val acquire = makeProducer.flatMap(Ref.of[F, Producer[String, Array[Byte]]](_))
    val release = (ref: Ref[F, Producer[String, Array[Byte]]]) =>
      ref.get.flatMap(p => Sync[F].blocking(p.close()))
    for {
      producerRef <- Resource.make(acquire)(release)
      ec1         <- createExecutionContext
      ec2         <- createExecutionContext
    } yield impl(config, makeProducer, producerRef, ec1, ec2)
  }

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  private def newProducer[F[_]: Sync](
    config: KafkaSinkConfig,
    authHandlerClass: String
  ): F[Producer[String, Array[Byte]]] = Sync[F].delay {
    val producerSettings = Map(
      "bootstrap.servers"                -> config.bootstrapServers,
      "sasl.login.callback.handler.class" -> authHandlerClass,
      "key.serializer"                   -> classOf[StringSerializer].getName,
      "value.serializer"                 -> classOf[ByteArraySerializer].getName
    ) ++ config.producerConf
    new KafkaProducer[String, Array[Byte]]((producerSettings: Map[String, AnyRef]).asJava)
  }

  private def impl[F[_]: Async](
    config: KafkaSinkConfig,
    makeProducer: F[Producer[String, Array[Byte]]],
    producerRef: Ref[F, Producer[String, Array[Byte]]],
    ecForSend: ExecutionContext,
    ecForWait: ExecutionContext
  ): Sink[F] = {
    // With idempotent producers (enable.idempotence=true), OUT_OF_ORDER_SEQUENCE_NUMBER from the
    // broker is not surfaced as OutOfOrderSequenceException to future.get(). Instead the client
    // enters an internal epoch-bump retry loop (KAFKA-7848 — unresolved, no fix version assigned).
    // This loop may not respect delivery.timeout.ms, meaning future.get() may never return unless
    // delivery.timeout.ms is explicitly configured in producerConf. When it does expire, the client
    // throws KafkaTimeoutException. We replace the producer on timeout only when idempotence is
    // enabled: with idempotence off, OUT_OF_ORDER_SEQUENCE_NUMBER cannot occur (it is an
    // idempotent-producer-only error), so a KafkaTimeoutException represents a genuine delivery
    // timeout where the outcome is uncertain and retrying risks duplicate delivery.
    //
    // IMPORTANT: callers must set delivery.timeout.ms in producerConf when using idempotent
    // producers to bound this loop. Without it, future.get() may block indefinitely.
    val idempotenceEnabled = config.producerConf.get("enable.idempotence").contains("true")

    new Sink[F] {

      def sink(batch: ListOfList[Sinkable]): F[Unit] =
        sendBatch(batch).recoverWith {
          case e: ExecutionException if requiresProducerReplacement(e, idempotenceEnabled) =>
            Logger[F].warn(
              s"Producer error on topic ${config.topicName} (${e.getCause.getClass.getSimpleName}): replacing producer and retrying batch"
            ) >> replaceProducer >> sendBatch(batch)
        }

      private def sendBatch(batch: ListOfList[Sinkable]): F[Unit] =
        producerRef.get.flatMap { producer =>
          val futures = Sync[F].delay {
            batch.asIterable.map { e =>
              val record = toProducerRecord(config, e)
              producer.send(record)
            }.toIndexedSeq
          }
          Async[F].evalOn(futures, ecForSend).flatMap { fs =>
            val await = Sync[F].delay {
              fs.foreach(_.get)
            }
            Async[F].evalOn(await, ecForWait)
          }
        }

      // Close the old producer and swap in a fresh one. The old producer is closed
      // after the swap so that isHealthy checks and concurrent sends always see a
      // valid producer reference.
      private def replaceProducer: F[Unit] =
        for {
          replacement <- makeProducer
          old         <- producerRef.getAndSet(replacement)
          _           <- Sync[F].blocking(old.close())
        } yield ()

      def isHealthy: F[Boolean] =
        producerRef.get.flatMap { producer =>
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
    }
  }

  private def requiresProducerReplacement(e: ExecutionException, idempotenceEnabled: Boolean): Boolean =
    e.getCause match {
      case _: OutOfOrderSequenceException   => true
      case _: InvalidProducerEpochException => true
      // For idempotent-only producers, OUT_OF_ORDER_SEQUENCE_NUMBER is not surfaced as
      // OutOfOrderSequenceException to future.get(). The client retries internally via an
      // epoch-bump loop until delivery.timeout.ms expires, then throws KafkaTimeoutException
      // (KAFKA-7848). We only replace on timeout when idempotence is enabled: with idempotence
      // off, delivery outcome on timeout is uncertain and retrying could produce duplicates.
      // KafkaTimeoutException after delivery.timeout.ms expiry is the observable symptom of
      // KAFKA-7848 with idempotence enabled. With idempotence disabled this error is a genuine
      // delivery timeout and must not be retried blindly.
      case _: KafkaTimeoutException if idempotenceEnabled => true
      case _                                              => false
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
