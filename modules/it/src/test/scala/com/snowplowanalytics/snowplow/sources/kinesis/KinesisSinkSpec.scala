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

import com.snowplowanalytics.snowplow.it.DockerPull
import com.snowplowanalytics.snowplow.it.kinesis._
import com.snowplowanalytics.snowplow.sinks.{ListOfList, Sink, Sinkable}

import Utils._
import org.specs2.specification.BeforeAll

class KinesisSinkSpec extends CatsResource[IO, (Region, LocalStackContainer, Sink[IO])] with SpecificationLike with BeforeAll {
  import KinesisSinkSpec._

  override val Timeout: FiniteDuration = 3.minutes
  override def beforeAll(): Unit = {
    DockerPull.pull(Localstack.image, Localstack.tag)
    super.beforeAll()
  }

  /** Resources which are shared across tests */
  override val resource: Resource[IO, (Region, LocalStackContainer, Sink[IO])] =
    for {
      region <- Resource.eval(IO.blocking((new DefaultAwsRegionProviderChain).getRegion))
      localstack <- Localstack.resource(region, KINESIS_INITIALIZE_STREAMS, KinesisSinkSpec.getClass.getSimpleName)
      testSink <- KinesisSink.resource[IO](getKinesisSinkConfig(localstack.getEndpoint)(testStream1Name))
    } yield (region, localstack, testSink)

  override def is = s2"""
  KinesisSinkSpec should
    write to output stream $e1
  """

  def e1 = withResource { case (region, localstack, testSink) =>
    val testPayload = "test-payload"
    val testInput   = ListOfList.ofItems(Sinkable(testPayload.getBytes(), Some("myPk"), Map(("", ""))))

    for {
      kinesisClient <- getKinesisClient(localstack.getEndpoint, region)
      _ <- testSink.sink(testInput)
      _ <- IO.sleep(3.seconds)
      result = getDataFromKinesis(kinesisClient, testStream1Name)
    } yield List(
      result.events must haveSize(1),
      result.events must beEqualTo(List(testPayload))
    )
  }
}

object KinesisSinkSpec {
  val testStream1Name = "test-sink-stream-1"
  val KINESIS_INITIALIZE_STREAMS: String =
    List(s"$testStream1Name:1").mkString(",")
}
