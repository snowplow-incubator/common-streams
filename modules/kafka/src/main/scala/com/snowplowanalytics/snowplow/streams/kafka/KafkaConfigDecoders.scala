/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams.kafka

import io.circe.Decoder

private[kafka] object KafkaConfigDecoders {

  /**
   * Decoder for the map of options we pass to the underlying Kafka client, i.e. `consumerConf` and
   * `producerConf`.
   *
   * An option whose value is `null` is treated as unset, so an application's hocon can remove an
   * option which we set by default in our reference.conf. For example, a deployment on Event Hubs
   * can set `"group.instance.id": null` to opt out of static consumer group membership.
   *
   * Because a `null` no longer fails to decode, we must check explicitly for the option which we
   * require the application to provide.
   *
   * The returned decoder must be summoned for exactly one field of a config, because the
   * required-key check rides on the `Map[String, String]` type: a second map field in the same case
   * class would silently inherit the same requirement.
   *
   * @param requiredKey
   *   an option which must be present with a non-null value, e.g. `group.id`
   */
  def clientConf(requiredKey: String): Decoder[Map[String, String]] =
    Decoder[Map[String, Option[String]]].emap { conf =>
      conf.get(requiredKey).flatten match {
        case Some(_) =>
          Right(conf.collect { case (key, Some(value)) => key -> value })
        case None =>
          Left(s"$requiredKey must be set to a non-null value")
      }
    }
}
