/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import io.circe._
import io.circe.config.syntax._
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import io.circe.generic.extras.Configuration

import java.net.URI
import java.time.Instant
import scala.concurrent.duration.FiniteDuration

import com.snowplowanalytics.snowplow.kinesis.BackoffPolicy

/**
 * Config to be supplied from the app's hocon
 *
 * @param appName
 *   Corresponds to the DynamoDB table name
 * @param workerIdentifier
 *   If a pod uses a consistent name, then whenever the pod restarts (e.g. after crashing or after a
 *   rollout) then the pod can re-claim leases that it previously owned before the restart
 * @param leaseDuration
 *   The KCL default for a lease is 10 seconds. If we increase this, then we can allow a pod longer
 *   to re-claim its old leases after a restart.
 * @param maxLeasesToStealAtOneTimeFactor
 *   Controls how to pick the max number of leases to steal at one time. The actual max number to
 *   steal is multiplied by the number of runtime processors. In order to avoid latency during scale
 *   up/down and pod-rotation, we want the app to be quick to acquire shard-leases to process. With
 *   bigger instances (more cores/processors) we tend to have more shard-leases per instance, so we
 *   increase how aggressively it acquires leases.
 *
 * Other params are self-explanatory
 */
case class KinesisSourceConfig(
  appName: String,
  streamName: String,
  workerIdentifier: String,
  initialPosition: KinesisSourceConfig.InitialPosition,
  retrievalMode: KinesisSourceConfig.Retrieval,
  customEndpoint: Option[URI],
  dynamodbCustomEndpoint: Option[URI],
  cloudwatchCustomEndpoint: Option[URI],
  leaseDuration: FiniteDuration,
  maxLeasesToStealAtOneTimeFactor: BigDecimal,
  checkpointThrottledBackoffPolicy: BackoffPolicy
)

object KinesisSourceConfig {

  sealed trait InitialPosition

  object InitialPosition {
    case object Latest extends InitialPosition
    case object TrimHorizon extends InitialPosition
    case class AtTimestamp(timestamp: Instant) extends InitialPosition

    private[KinesisSourceConfig] def decoder(implicit c: Configuration) = deriveConfiguredDecoder[InitialPosition]
  }

  sealed trait Retrieval

  object Retrieval {
    case class Polling(maxRecords: Int) extends Retrieval
    case object FanOut extends Retrieval

    private[KinesisSourceConfig] def decoder(implicit c: Configuration) = deriveConfiguredDecoder[Retrieval]
  }

  implicit val decoder: Decoder[KinesisSourceConfig] = {
    implicit val config: Configuration = Configuration.default.withDiscriminator("type")
    val alternativeConfig              = config.withScreamingSnakeCaseConstructorNames

    implicit val initPosDecoder = InitialPosition.decoder(config).or(InitialPosition.decoder(alternativeConfig))
    implicit val retrieval      = Retrieval.decoder(config).or(Retrieval.decoder(alternativeConfig))
    deriveConfiguredDecoder[KinesisSourceConfig]
  }
}
