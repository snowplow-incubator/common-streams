/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.streams

import cats.effect.Resource

trait Factory[F[_], SourceConfig, SinkConfig] {

  def sink(config: SinkConfig): Resource[F, Sink[F]]

  def source(config: SourceConfig): Resource[F, SourceAndAck[F]]
}
