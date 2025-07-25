/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.nsq.sink

import scala.jdk.CollectionConverters._

import cats.effect.{Async, Resource, Sync}

import retry.syntax.all._

import com.sproutsocial.nsq.{Client, ListBasedBalanceStrategy, Publisher}

import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.streams.{ListOfList, Sink, Sinkable}
import com.snowplowanalytics.snowplow.streams.nsq.Retries
import com.snowplowanalytics.snowplow.streams.nsq.NsqSinkConfig

private[nsq] object NsqSink {

  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def resource[F[_]: Async](config: NsqSinkConfig): Resource[F, Sink[F]] =
    mkPublisher[F](config).map { p =>
      new Sink[F] {
        def sink(batch: ListOfList[Sinkable]): F[Unit] =
          sinkBatch[F](p, config, batch)

        def isHealthy: F[Boolean] =
          Sync[F].pure(true)
      }
    }

  private def sinkBatch[F[_]: Async](
    publisher: Publisher,
    config: NsqSinkConfig,
    batch: ListOfList[Sinkable]
  ): F[Unit] =
    Sync[F]
      .blocking {
        batch
          .group(recordLimit = Int.MaxValue, sizeLimit = config.byteLimit, getRecordSize = _.bytes.length)
          .foreach { l =>
            val records = l.map(_.bytes).asJava
            publisher.publish(config.topic, records)
          }
      }
      .retryingOnAllErrors(
        policy = Retries.fullJitter[F](config.backoffPolicy),
        onError = (exception, retryDetails) =>
          Logger[F]
            .error(exception)(
              s"Writing to ${config.topic} errored (${retryDetails.retriesSoFar} retries from cats-retry)"
            )
      )

  private def mkPublisher[F[_]: Sync](config: NsqSinkConfig): Resource[F, Publisher] =
    Resource.make(
      Sync[F].delay {
        // Explicitly creating round robin strategy with one nsqd instance in order to not get warning
        // log from SingleNsqdBalanceStrategy
        new Publisher(
          Client.getDefaultClient,
          ListBasedBalanceStrategy.getRoundRobinStrategyBuilder(List(s"${config.nsqdHost}:${config.nsqdPort}").asJava)
        )
      }
    ) { publisher =>
      Sync[F].blocking {
        publisher.stop()
      }
    }
}
