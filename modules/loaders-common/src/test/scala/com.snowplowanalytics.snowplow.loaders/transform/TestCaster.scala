/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import cats.data.NonEmptyVector
import io.circe.{Json, JsonObject}

import java.time.{Instant, LocalDate}

import com.snowplowanalytics.iglu.schemaddl.parquet.{Caster, Type}

object TestCaster extends Caster[Json] {

  override def nullValue: Json                = Json.Null
  override def jsonValue(v: Json): Json       = v
  override def stringValue(v: String): Json   = Json.fromString(v)
  override def booleanValue(v: Boolean): Json = Json.fromBoolean(v)
  override def intValue(v: Int): Json         = Json.fromInt(v)
  override def longValue(v: Long): Json       = Json.fromLong(v)
  override def doubleValue(v: Double): Json   = Json.fromDoubleOrNull(v)
  override def decimalValue(unscaled: BigInt, details: Type.Decimal): Json =
    Json.fromBigDecimal(BigDecimal(unscaled, details.scale))
  override def timestampValue(v: Instant): Json   = Json.fromString(v.toString)
  override def dateValue(v: LocalDate): Json      = Json.fromString(v.toString)
  override def arrayValue(vs: Vector[Json]): Json = Json.fromValues(vs)
  override def structValue(vs: NonEmptyVector[Caster.NamedValue[Json]]): Json =
    Json.fromJsonObject(JsonObject.fromIterable(vs.toVector.map(nv => nv.name -> nv.value)))

}
