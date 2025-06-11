/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kinesis.sink

import cats.implicits._
import cats.effect.implicits._
import cats.effect.{Async, Ref, Resource, Sync}

import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import retry.syntax.all._

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode
import software.amazon.awssdk.services.kinesis.model.{
  ListShardsRequest,
  PutRecordsRequest,
  PutRecordsRequestEntry,
  PutRecordsResponse,
  ShardFilter,
  ShardFilterType
}

import com.snowplowanalytics.snowplow.streams.kinesis.{BackoffPolicy, KinesisSinkConfig, Retries}

import java.util.UUID
import java.nio.charset.StandardCharsets.UTF_8

import scala.jdk.CollectionConverters._

import com.snowplowanalytics.snowplow.streams.{ListOfList, Sink, Sinkable}

private[kinesis] object KinesisSink {

  def resource[F[_]: Async](config: KinesisSinkConfig, client: SdkAsyncHttpClient): Resource[F, Sink[F]] =
    mkProducer[F](config, client).map { p =>
      new Sink[F] {
        def sink(batch: ListOfList[Sinkable]): F[Unit] =
          writeToKinesis[F](
            config.throttledBackoffPolicy,
            RequestLimits(config.recordLimit, config.byteLimit),
            p,
            config.streamName,
            batch
          )

        def pingForHealth: F[Boolean] =
          checkShardsAreOpen(config.streamName, p)
      }
    }

  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  private def mkProducer[F[_]: Sync](config: KinesisSinkConfig, client: SdkAsyncHttpClient): Resource[F, KinesisAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        val builder = KinesisAsyncClient.builder
          .httpClient(client)
          .defaultsMode(DefaultsMode.AUTO)
        config.customEndpoint.foreach(uri => builder.endpointOverride(uri))
        builder.build()
      }
    }

  private def checkShardsAreOpen[F[_]: Async](streamName: String, client: KinesisAsyncClient): F[Boolean] = {
    def go(nextToken: Option[String]): F[Boolean] =
      Async[F]
        .fromCompletableFuture {
          Sync[F].delay {
            val request = ListShardsRequest.builder
              .streamName(streamName)
              .shardFilter {
                ShardFilter.builder.`type`(ShardFilterType.AT_LATEST).build
              }
              .nextToken(nextToken.orNull)
              .build
            client.listShards(request)
          }
        }
        .flatMap { response =>
          if (response.hasShards)
            Logger[F].info(s"Confirmed stream $streamName has open shards").as(true)
          else {
            Option(response.nextToken) match {
              case Some(t) =>
                go(Some(t))
              case None =>
                Logger[F].warn(s"Stream $streamName has no open shards").as(false)
            }
          }
        }

    Logger[F].info(s"Checking whether stream $streamName has open shards") >> go(None)
  }

  private def toKinesisRecords(records: ListOfList[Sinkable]): ListOfList[PutRecordsRequestEntry] =
    records.mapUnordered { r =>
      val data = SdkBytes.fromByteArrayUnsafe(r.bytes)
      val prre = PutRecordsRequestEntry
        .builder()
        .partitionKey(r.partitionKey.getOrElse(UUID.randomUUID.toString()))
        .data(data)
        .build()
      prre
    }

  /**
   * The result of trying to write a batch to kinesis
   * @param nextBatchAttempt
   *   Records to re-package into another batch, either because of throttling or an internal error
   * @param hadNonThrottleErrors
   *   Whether at least one of failures is not because of throttling
   * @param exampleInternalError
   *   A message to help with logging
   */
  private case class TryBatchResult(
    nextBatchAttempt: List[PutRecordsRequestEntry],
    hadNonThrottleErrors: Boolean,
    exampleInternalError: Option[String]
  )

  private object TryBatchResult {

    /**
     * The build method creates a TryBatchResult, which:
     *
     *   - Returns an empty list and false for hadNonThrottleErrors if everything was successful
     *   - Returns the list of failed requests and true for hadNonThrottleErrors if we encountered
     *     any errors that weren't throttles
     *   - Returns the list of failed requests and false for hadNonThrottleErrors if we encountered
     *     only throttling
     */
    def build(records: List[PutRecordsRequestEntry], prr: PutRecordsResponse): TryBatchResult =
      if (prr.failedRecordCount().toInt =!= 0)
        records
          .zip(prr.records().asScala)
          .foldLeft(TryBatchResult(Nil, false, None)) { case (acc, (orig, recordResult)) =>
            Option(recordResult.errorCode()) match {
              // If the record had no error, treat as success
              case None => acc
              // If it had a throughput exception, mark that and provide the original
              case Some("ProvisionedThroughputExceededException") =>
                acc.copy(nextBatchAttempt = orig :: acc.nextBatchAttempt)
              // If any other error, mark success and throttled false for this record, and provide the original
              case Some(_) =>
                TryBatchResult(orig :: acc.nextBatchAttempt, true, acc.exampleInternalError.orElse(Option(recordResult.errorMessage())))

            }
          }
      else
        TryBatchResult(Nil, false, None)
  }

  /**
   * Try writing a batch, and returns a list of the failures to be retried:
   *
   * If we are not throttled by kinesis, then the list is empty. If we are throttled by kinesis, the
   * list contains throttled records and records that gave internal errors. If there is an
   * exception, or if all records give internal errors, then we retry using the policy.
   */
  private def tryWriteToKinesis[F[_]: Async](
    streamName: String,
    kinesis: KinesisAsyncClient,
    records: List[PutRecordsRequestEntry]
  ): F[List[PutRecordsRequestEntry]] =
    Logger[F].debug(s"Writing ${records.size} records to ${streamName}") *>
      Async[F]
        .fromCompletableFuture {
          Sync[F].delay {
            val putRecordsRequest = {
              val prr = PutRecordsRequest.builder()
              prr
                .streamName(streamName)
                .records(records.asJava)
              prr.build()
            }
            kinesis.putRecords(putRecordsRequest)
          }
        }
        .map(TryBatchResult.build(records, _))
        .flatMap { result =>
          // If we encountered non-throttle errors, raise an exception. Otherwise, return all the requests that should
          // be manually retried due to throttling
          if (result.hadNonThrottleErrors)
            Sync[F].raiseError(new RuntimeException(failureMessageForInternalErrors(records, streamName, result)))
          else
            result.nextBatchAttempt.pure[F]
        }

  private def writeToKinesis[F[_]: Async](
    throttlingErrorsPolicy: BackoffPolicy,
    requestLimits: RequestLimits,
    kinesis: KinesisAsyncClient,
    streamName: String,
    records: ListOfList[Sinkable]
  ): F[Unit] = {
    val policyForThrottling = Retries.forThrottling[F](throttlingErrorsPolicy)

    // First, tryWriteToKinesis - the AWS SDK will handle retries. If there are still failures after that, it will:
    // - return messages for retries if we only hit throttliing
    // - raise an error if we still have non-throttle failures after the SDK has carried out retries
    def runAndCaptureFailures(ref: Ref[F, ListOfList[PutRecordsRequestEntry]]): F[ListOfList[PutRecordsRequestEntry]] =
      for {
        records <- ref.get
        failures <- records
                      .group(requestLimits.recordLimit, requestLimits.bytesLimit, getRecordSize)
                      .parTraverse(g => tryWriteToKinesis(streamName, kinesis, g))
        listOfList = ListOfList.of(failures)
        _ <- ref.set(listOfList)
      } yield listOfList
    for {
      ref <- Ref.of[F, ListOfList[PutRecordsRequestEntry]](toKinesisRecords(records))
      failures <- runAndCaptureFailures(ref)
                    .retryingOnFailures(
                      policy        = policyForThrottling,
                      wasSuccessful = entries => Sync[F].pure(entries.isEmpty),
                      onFailure = { case (result, retryDetails) =>
                        val msg = failureMessageForThrottling(result, streamName)
                        Logger[F].warn(s"$msg (${retryDetails.retriesSoFar} retries from cats-retry)")
                      }
                    )
      _ <- if (failures.isEmpty) Sync[F].unit
           else Sync[F].raiseError(new RuntimeException(failureMessageForThrottling(failures, streamName)))
    } yield ()
  }

  private final case class RequestLimits(recordLimit: Int, bytesLimit: Int)

  private def getRecordSize(record: PutRecordsRequestEntry) =
    record.data.asByteArrayUnsafe().length + record.partitionKey().getBytes(UTF_8).length

  private def failureMessageForInternalErrors(
    records: List[PutRecordsRequestEntry],
    streamName: String,
    result: TryBatchResult
  ): String = {
    val exampleMessage = result.exampleInternalError.getOrElse("none")
    s"Writing ${records.size} records to $streamName errored with internal failures. Example error message [$exampleMessage]"
  }

  private def failureMessageForThrottling(
    records: ListOfList[PutRecordsRequestEntry],
    streamName: String
  ): String =
    s"Exceeded Kinesis provisioned throughput: ${records.size} records failed writing to $streamName."
}
