/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks.kinesis

import cats.effect.{IO, Resource}
import cats.effect.testing.specs2.CatsResource

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import org.specs2.mutable.SpecificationLike

import org.testcontainers.containers.localstack.LocalStackContainer

import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.regions.Region

import com.snowplowanalytics.snowplow.it.kinesis._
import com.snowplowanalytics.snowplow.sinks.Sinkable

import Utils._

class KinesisSinkSpec extends CatsResource[IO, (Region, LocalStackContainer, String => KinesisSinkConfig)] with SpecificationLike {

  override val Timeout: FiniteDuration = 3.minutes

  /** Resources which are shared across tests */
  override val resource: Resource[IO, (Region, LocalStackContainer, String => KinesisSinkConfig)] =
    for {
      region <- Resource.eval(IO.blocking((new DefaultAwsRegionProviderChain).getRegion))
      localstack <- Localstack.resource(region, KinesisSinkSpec.getClass.getSimpleName)
    } yield (region, localstack, getKinesisSinkConfig(localstack.getEndpoint)(_))

  override def is = s2"""
  KinesisSinkSpec should
    write to output stream $e1
  """

  def e1 = withResource { case (region, localstack, getKinesisSinkConfig) =>
    val testStream1Name = "test-sink-stream-1"
    val testPayload     = "test-payload"
    val testInput       = List(Sinkable(testPayload.getBytes(), Some("myPk"), Map(("", ""))))

    val kinesisClient = getKinesisClient(localstack.getEndpoint, region)
    createAndWaitForKinesisStream(kinesisClient, testStream1Name, 1): Unit
    val testSinkResource = KinesisSink.resource[IO](getKinesisSinkConfig(testStream1Name))

    for {
      _ <- testSinkResource.use(testSink => testSink.sink(testInput))
      _ <- IO.sleep(3.seconds)
      result = getDataFromKinesis(kinesisClient, testStream1Name)
    } yield List(
      result.events must haveSize(1),
      result.events must beEqualTo(List(testPayload))
    )
  }
}

object KinesisSinkSpec {}
