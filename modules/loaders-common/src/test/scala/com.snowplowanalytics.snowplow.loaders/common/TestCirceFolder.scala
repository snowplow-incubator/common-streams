/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.common

import io.circe.{Json, JsonNumber, JsonObject}

object TestCirceFolder extends Json.Folder[Json] {

  override def onNull: Json                       = Json.Null
  override def onBoolean(value: Boolean): Json    = Json.fromBoolean(value)
  override def onNumber(value: JsonNumber): Json  = Json.fromJsonNumber(value)
  override def onString(value: String): Json      = Json.fromString(value)
  override def onArray(value: Vector[Json]): Json = Json.fromValues(value)
  override def onObject(value: JsonObject): Json  = Json.fromJsonObject(value)

}
