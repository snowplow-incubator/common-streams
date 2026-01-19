/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.it

import cats.Id
import cats.effect.{IO, Ref}

import scala.jdk.CollectionConverters._

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{GetRecordsRequest, GetShardIteratorRequest, PutRecordRequest, PutRecordResponse}

import com.snowplowanalytics.snowplow.streams.{EventProcessor, TokenedEvents}
import com.snowplowanalytics.snowplow.streams.kinesis.{
  BackoffPolicy,
  KinesisHttpSourceConfig,
  KinesisSinkConfig,
  KinesisSinkConfigM,
  KinesisSourceConfig
}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.concurrent.duration.DurationLong

import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest

object Utils {

  case class ReceivedEvents(events: List[String])

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
    streamName: String
  ): ReceivedEvents = {

    val descStreamResp = client.describeStream(DescribeStreamRequest.builder().streamName(streamName).build()).get

    // We're assuming only one shard here.
    // Any future test with multiple shards requires us to create one iterator per shard
    val shIterRequest = GetShardIteratorRequest
      .builder()
      .streamName(streamName)
      .shardIteratorType("TRIM_HORIZON")
      .shardId(descStreamResp.streamDescription.shards.get(0).shardId)
      .build();

    val shIter = client.getShardIterator(shIterRequest).get.shardIterator

    val request = GetRecordsRequest
      .builder()
      .streamARN(descStreamResp.streamDescription().streamARN())
      .shardIterator(shIter)
      .build()

    val out =
      ReceivedEvents(client.getRecords(request).get().records().asScala.toList.map(record => new String(record.data.asByteArray())))
    out
  }

  def getKinesisSourceConfig(endpoint: URI)(streamName: String): KinesisHttpSourceConfig =
    KinesisHttpSourceConfig(
      kinesis = KinesisSourceConfig(
        UUID.randomUUID().toString,
        streamName,
        UUID.randomUUID.toString,
        KinesisSourceConfig.InitialPosition.TrimHorizon,
        KinesisSourceConfig.Retrieval.Polling(1),
        Some(endpoint),
        Some(endpoint),
        Some(endpoint),
        10.seconds,
        BigDecimal(1.0),
        BackoffPolicy(100.millis, 1.second),
        10.seconds,
        10,
        15.seconds
      ),
      http = None
    )

  def getKinesisSinkConfig(endpoint: URI)(streamName: String): KinesisSinkConfig = KinesisSinkConfigM[Id](
    streamName,
    BackoffPolicy(1.second, 1.second),
    1000,
    1000000,
    Some(endpoint),
    10
  )

  def testProcessor(ref: Ref[IO, List[ReceivedEvents]]): EventProcessor[IO] =
    _.evalMap { case TokenedEvents(events, token) =>
      val parsed = events.map(byteBuffer => StandardCharsets.UTF_8.decode(byteBuffer).toString)
      for {
        _ <- ref.update(_ :+ ReceivedEvents(parsed.toList))
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
