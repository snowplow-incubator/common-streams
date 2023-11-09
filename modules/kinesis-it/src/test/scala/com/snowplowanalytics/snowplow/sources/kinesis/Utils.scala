/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.it.kinesis

import cats.effect.{IO, Ref}

// import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
// import scala.jdk.FutureConverters._
// import scala.concurrent.Await

import eu.timepit.refined.types.numeric.PosInt

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{GetRecordsRequest, PutRecordRequest, PutRecordResponse}

import com.snowplowanalytics.snowplow.sources.{EventProcessor, TokenedEvents}
import com.snowplowanalytics.snowplow.sources.kinesis.KinesisSourceConfig
import com.snowplowanalytics.snowplow.sinks.kinesis.{BackoffPolicy, KinesisSinkConfig}
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.time.Instant
import java.util.concurrent.TimeUnit

object Utils {

  case class ReceivedEvents(events: List[String], tstamp: Option[Instant])

  def putDataToKinesis(
    client: KinesisAsyncClient,
    streamName: String,
    data: String
  ): IO[PutRecordResponse] = {
    val record = PutRecordRequest
      .builder()
      .streamName(streamName)
      .data(SdkBytes.fromUtf8String(data))
      .partitionKey(UUID.randomUUID().toString)
      .build()

    IO.blocking(client.putRecord(record).get())
  }

  /**
   * getDataFromKinesis gets the last 1000 records from kinesis, stringifies the datta it found, and
   * returns a ReceivedEvents It can be called at the end of simple tests to return data from a
   * Kinesis stream.
   *
   * If required in future, where more data is used we might amend it to poll the stream for data
   * and return everything it finds after a period without any data.
   */
  def getDataFromKinesis(
    client: KinesisAsyncClient,
    region: String,
    streamName: String
  ): ReceivedEvents = {

    val shIterRequest = GetShardIteratorRequest
      .builder()
      .streamName("test-sink-stream-1")
      .shardIteratorType("TRIM_HORIZON")
      .shardId("shardId-000000000000")
      .build();

    val shIter = client.getShardIterator(shIterRequest).get.shardIterator

    val request = GetRecordsRequest
      .builder()
      .streamARN("arn:aws:kinesis:%s:000000000000:stream/%s".format(region, streamName))
      .shardIterator(shIter)
      .build()

    val out =
      ReceivedEvents(client.getRecords(request).get().records().asScala.toList.map(record => new String(record.data.asByteArray())), None)
    out
  }

  def getKinesisSourceConfig(endpoint: URI)(streamName: String): KinesisSourceConfig = KinesisSourceConfig(
    UUID.randomUUID().toString,
    streamName,
    KinesisSourceConfig.InitialPosition.TrimHorizon,
    KinesisSourceConfig.Retrieval.Polling(1),
    PosInt.unsafeFrom(1),
    Some(endpoint),
    Some(endpoint),
    Some(endpoint)
  )

  def getKinesisSinkConfig(endpoint: URI)(streamName: String): KinesisSinkConfig = KinesisSinkConfig(
    streamName,
    BackoffPolicy(FiniteDuration(1, TimeUnit.SECONDS), FiniteDuration(1, TimeUnit.SECONDS), None),
    1000,
    1000000,
    Some(endpoint)
  )

  def testProcessor(ref: Ref[IO, List[ReceivedEvents]]): EventProcessor[IO] =
    _.evalMap { case TokenedEvents(events, token, tstamp) =>
      val parsed = events.map(byteBuffer => StandardCharsets.UTF_8.decode(byteBuffer).toString)
      for {
        _ <- ref.update(_ :+ ReceivedEvents(parsed, tstamp))
      } yield token
    }

  def getKinesisClient(endpoint: URI, region: Region): IO[KinesisAsyncClient] =
    IO(
      KinesisAsyncClient
        .builder()
        .endpointOverride(endpoint)
        .region(region)
        .build()
    )
}
