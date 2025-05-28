/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.pubsub

import cats.Show
import io.circe.Decoder
import io.circe.generic.semiauto._
import io.circe.config.syntax._
import com.google.pubsub.v1.ProjectSubscriptionName

import scala.concurrent.duration.FiniteDuration

/**
 * Configures the Pubsub Source
 *
 * @param subscription
 *   Identifier of the pubsub subscription
 * @param parallelPullFactor
 *   Controls how many RPCs may be opened concurrently from the client to the PubSub server. The
 *   maximum number of RPCs is equal to this factor multiplied by the number of available cpu cores.
 *   Increasing this factor can increase the rate of events provided by the Source to the
 *   application.
 * @param durationPerAckExtension
 *   Ack deadlines are extended for this duration. For common-streams apps this should be set
 *   slightly larger than the maximum time we expect between app receiving the message and acking
 *   the message. If a message is ever held by the app for longer than expected, then it's ok: the
 *   Source will re-extend the ack deadline.
 * @param minRemainingAckDeadline
 *   Controls when ack deadlines are re-extended, for a message that is close to exceeding its ack
 *   deadline.. For example, if `durationPerAckExtension` is `60 seconds` and
 *   `minRemainingAckDeadline` is `0.1` then the Source will wait until there is `6 seconds` left of
 *   the remining deadline, before re-extending the message deadline.
 * @param maxMessagesPerPull
 *   How many pubsub messages to pull from the server in a single request.
 * @param debounceRequests
 *   Adds an artifical delay between consecutive requests to pubsub for more messages. Under some
 *   circumstances, this was found to slightly alleviate a problem in which pubsub might re-deliver
 *   the same messages multiple times.
 */
case class PubsubSourceConfig(
  subscription: PubsubSourceConfig.Subscription,
  parallelPullFactor: BigDecimal,
  durationPerAckExtension: FiniteDuration,
  minRemainingAckDeadline: BigDecimal,
  maxMessagesPerPull: Int,
  debounceRequests: FiniteDuration
)

object PubsubSourceConfig {

  case class Subscription(projectId: String, subscriptionId: String)

  object Subscription {
    implicit def show: Show[Subscription] = Show[Subscription] { s =>
      ProjectSubscriptionName.of(s.projectId, s.subscriptionId).toString
    }
  }

  private implicit def subscriptionDecoder: Decoder[Subscription] =
    Decoder.decodeString
      .map(_.split("/"))
      .emap {
        case Array("projects", projectId, "subscriptions", subscriptionId) =>
          Right(Subscription(projectId, subscriptionId))
        case _ =>
          Left("Expected format: projects/<project>/subscriptions/<subscription>")
      }

  implicit def decoder: Decoder[PubsubSourceConfig] = deriveDecoder[PubsubSourceConfig]
}
