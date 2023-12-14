/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import cats.effect.{IO, Ref, Resource}
import cats.effect.testing.specs2.CatsResource

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import org.specs2.mutable.SpecificationLike

import org.testcontainers.containers.localstack.LocalStackContainer

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, SourceAndAck}
import com.snowplowanalytics.snowplow.sources.EventProcessingConfig.NoWindowing
import com.snowplowanalytics.snowplow.it.kinesis._

import java.time.Instant
import Utils._

class KinesisSourceSpec extends CatsResource[IO, (LocalStackContainer, KinesisAsyncClient, SourceAndAck[IO])] with SpecificationLike {
  import KinesisSourceSpec._

  override val Timeout: FiniteDuration = 3.minutes

  /** Resources which are shared across tests */
  override val resource: Resource[IO, (LocalStackContainer, KinesisAsyncClient, SourceAndAck[IO])] =
    for {
      region <- Resource.eval(IO.blocking((new DefaultAwsRegionProviderChain).getRegion))
      localstack <- Localstack.resource(region, KINESIS_INITIALIZE_STREAMS, KinesisSourceSpec.getClass.getSimpleName)
      kinesisClient <- Resource.eval(getKinesisClient(localstack.getEndpoint, region))
      sourceAndAck <- KinesisSource.build[IO](getKinesisSourceConfig(localstack.getEndpoint)(testStream1Name))
    } yield (localstack, kinesisClient, sourceAndAck)

  override def is = s2"""
  KinesisSourceSpec should
    read from input stream $e1
  """

  def e1 = withResource { case (_, kinesisClient, sourceAndAck) =>
    val testPayload = "test-payload"

    for {
      refProcessed <- Ref[IO].of[List[ReceivedEvents]](Nil)
      t1 <- IO.realTimeInstant
      _ <- putDataToKinesis(kinesisClient, testStream1Name, testPayload)
      t2 <- IO.realTimeInstant
      processingConfig = new EventProcessingConfig(NoWindowing)
      stream           = sourceAndAck.stream(processingConfig, testProcessor(refProcessed))
      fiber <- stream.compile.drain.start
      _ <- IO.sleep(2.minutes)
      processed <- refProcessed.get
      _ <- fiber.cancel
    } yield List(
      processed must haveSize(1),
      processed.head.events must beEqualTo(List(testPayload)),
      processed.head.tstamp must beSome { tstamp: Instant =>
        tstamp.toEpochMilli must beBetween(t1.toEpochMilli, t2.toEpochMilli)
      }
    ).reduce(_ and _)
  }
}

object KinesisSourceSpec {
  val testStream1Name = "test-source-stream-1"
  val KINESIS_INITIALIZE_STREAMS: String =
    List(s"$testStream1Name:1").mkString(",")
}
