/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks.kinesis

import cats.implicits._
import cats.Parallel
import cats.effect.{Async, Resource, Sync}
import cats.effect.kernel.Ref

import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import retry.syntax.all._

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry, PutRecordsResponse}

import com.snowplowanalytics.snowplow.kinesis.{BackoffPolicy, Retries}

import java.net.URI
import java.util.UUID
import java.nio.charset.StandardCharsets.UTF_8

import scala.jdk.CollectionConverters._

import com.snowplowanalytics.snowplow.sinks.{ListOfList, Sink, Sinkable}

object KinesisSink {

  def resource[F[_]: Parallel: Async](config: KinesisSinkConfig): Resource[F, Sink[F]] =
    mkProducer[F](config).map { p =>
      Sink(
        writeToKinesis[F](
          config.throttledBackoffPolicy,
          RequestLimits(config.recordLimit, config.byteLimit),
          p,
          config.streamName,
          _
        )
      )
    }

  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  private def buildKinesisClient(customEndpoint: Option[URI], region: Region): KinesisClient = {
    val builder = KinesisClient.builder().region(region).defaultsMode(DefaultsMode.AUTO)
    customEndpoint.foreach(e => builder.endpointOverride(e))
    builder.build()
  }

  private def mkProducer[F[_]: Sync](config: KinesisSinkConfig): Resource[F, KinesisClient] = {
    val make = Sync[F].delay(buildKinesisClient(config.customEndpoint, (new DefaultAwsRegionProviderChain).getRegion()))

    Resource.make(make) { producer =>
      Sync[F].blocking {
        producer.close()
      }
    }
  }

  /**
   * This function takes a list of records and splits it into several lists, where each list is as
   * big as possible with respecting the record limit and the size limit.
   */
  private[kinesis] def group[A](
    records: ListOfList[A],
    recordLimit: Int,
    sizeLimit: Int,
    getRecordSize: A => Int
  ): List[List[A]] = {
    case class Batch(
      size: Int,
      count: Int,
      records: List[A]
    )

    records
      .foldLeft(List.empty[Batch]) { case (acc, record) =>
        val recordSize = getRecordSize(record)
        acc match {
          case head :: tail =>
            if (head.count + 1 > recordLimit || head.size + recordSize > sizeLimit)
              List(Batch(recordSize, 1, List(record))) ++ List(head) ++ tail
            else
              List(Batch(head.size + recordSize, head.count + 1, record :: head.records)) ++ tail
          case Nil =>
            List(Batch(recordSize, 1, List(record)))
        }
      }
      .map(_.records)
  }

  private def putRecords(
    kinesis: KinesisClient,
    streamName: String,
    records: List[PutRecordsRequestEntry]
  ): PutRecordsResponse = {
    val putRecordsRequest = {
      val prr = PutRecordsRequest.builder()
      prr
        .streamName(streamName)
        .records(records.asJava)
      prr.build()
    }
    kinesis.putRecords(putRecordsRequest)
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
  private def tryWriteToKinesis[F[_]: Sync](
    streamName: String,
    kinesis: KinesisClient,
    records: List[PutRecordsRequestEntry]
  ): F[List[PutRecordsRequestEntry]] =
    Logger[F].debug(s"Writing ${records.size} records to ${streamName}") *>
      Sync[F]
        .blocking(putRecords(kinesis, streamName, records))
        .map(TryBatchResult.build(records, _))
        .flatMap { result =>
          // If we encountered non-throttle errors, raise an exception. Otherwise, return all the requests that should
          // be manually retried due to throttling
          if (result.hadNonThrottleErrors)
            Sync[F].raiseError(new RuntimeException(failureMessageForInternalErrors(records, streamName, result)))
          else
            result.nextBatchAttempt.pure[F]
        }

  private def writeToKinesis[F[_]: Parallel: Async](
    throttlingErrorsPolicy: BackoffPolicy,
    requestLimits: RequestLimits,
    kinesis: KinesisClient,
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
        failures <- group(records, requestLimits.recordLimit, requestLimits.bytesLimit, getRecordSize)
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
