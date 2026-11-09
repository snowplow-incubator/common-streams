/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import cats.Monoid
import cats.implicits._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Locale

/**
 * Identifier for a family of versioned Snowplow entities which are treated as a common entity when
 * writing to the table
 *
 * E.g. unstruct events with types 1-0-0, 1-0-1, and 1-1-0 are treated as the same TabledEntity
 *
 * @param entityType
 *   whether this is a unstruct event or context
 * @param vendor
 *   the Iglu schema vendor
 * @param schemaName
 *   the Iglu schema name
 * @param model
 *   the Iglu schema model number, i.e. 1-*-*
 */
case class TabledEntity(
  entityType: TabledEntity.EntityType,
  vendor: String,
  schemaName: String,
  model: Int
)

object TabledEntity {
  sealed trait EntityType
  case object UnstructEvent extends EntityType
  case object Context extends EntityType

  /**
   * Extracts which TabledEntities will need to be included in the batch for this event, and the
   * corresponding sub-versions (e.g. *-1-1)
   */
  def forEvent(event: Event): Map[TabledEntity, Set[SchemaSubVersion]] = {
    val ue = event.unstruct_event.data.map(sdj => Map(forUnstructEvent(sdj.schema) -> Set(keyToSubVersion(sdj.schema))))

    val contexts = (event.contexts.data ++ event.derived_contexts.data).map { sdj =>
      Map(forContext(sdj.schema) -> Set(keyToSubVersion(sdj.schema)))
    }

    Monoid[Map[TabledEntity, Set[SchemaSubVersion]]].combineAll(contexts ++ ue)
  }

  def toSchemaKey(te: TabledEntity, subver: SchemaSubVersion): SchemaKey = {
    val ver = SchemaVer.Full(te.model, subver._1, subver._2)
    SchemaKey(vendor = te.vendor, name = te.schemaName, format = "jsonschema", version = ver)
  }

  /**
   * The name of this entity's column in the output of [[Transform.transformEventUnstructured]].
   *
   * This is a contract with every table a loader has already created: changing the output for any
   * input makes a loader add a duplicate column for an existing schema family. The golden examples
   * in `ColumnNameSpec` must never change.
   *
   * A name within `maxLength` is exactly the Analytics SDK's `transformSchema` name, with `ctx_` /
   * `ue_` in place of `contexts_` / `unstruct_event_` if `useShortPrefixes`. Like the SDK, it
   * splits PascalCase in the schema name but not in the vendor.
   *
   * A longer name is shortened by middle-hashing the vendor and, if the vendor would drop below
   * [[MinHashedLength]], the schema name too. The model is never hashed. The hash is over the raw
   * Iglu vendor or name, so components that normalise to the same text still get different hashes;
   * in SQL it is `left(md5(<raw component>), 8)`.
   *
   * `com.acme_foo/bar` and `com.acme/foo_bar` get the same name, as in the SDK. That collision is
   * accepted: the transform puts both families in one column.
   *
   * @param maxLength
   *   The destination's identifier length limit. It must be at least [[MinMaxLength]], which is
   *   enough to name any entity.
   * @param useShortPrefixes
   *   Whether to use the `ctx_` and `ue_` prefixes instead of `contexts_` and `unstruct_event_`
   */
  def columnName(
    te: TabledEntity,
    maxLength: Int,
    useShortPrefixes: Boolean
  ): String = {
    require(maxLength >= MinMaxLength, s"maxLength must be at least $MinMaxLength, but was $maxLength")
    val prefix = (te.entityType, useShortPrefixes) match {
      case (UnstructEvent, false) => "unstruct_event_"
      case (Context, false)       => "contexts_"
      case (UnstructEvent, true)  => "ue_"
      case (Context, true)        => "ctx_"
    }
    val vendor = normaliseVendor(te.vendor)
    val name   = normaliseName(te.schemaName)
    val suffix = s"_${te.model}"

    // The characters left for `<vendor>_<name>`
    val budget = maxLength - prefix.length - suffix.length

    val shortened =
      if (vendor.length + 1 + name.length <= budget)
        s"${vendor}_$name"
      else if (budget - 1 - name.length >= MinHashedLength)
        s"${middleHash(vendor, te.vendor, budget - 1 - name.length)}_$name"
      else {
        val shortVendor = if (vendor.length <= MinHashedLength) vendor else middleHash(vendor, te.vendor, MinHashedLength)
        s"${shortVendor}_${middleHash(name, te.schemaName, budget - 1 - shortVendor.length)}"
      }

    s"$prefix$shortened$suffix"
  }

  /** The length of the hash in a middle-hashed vendor or name */
  val HashLength: Int = 8

  /** The length of the shortest middle-hashed component: one character either side of the hash */
  val MinHashedLength: Int = HashLength + 4

  /**
   * The smallest `maxLength` that [[columnName]] accepts: the longest prefix, two components of
   * [[MinHashedLength]], two separators and the longest model
   */
  val MinMaxLength: Int = "unstruct_event_".length + 2 * MinHashedLength + 2 + Int.MaxValue.toString.length

  private val dotsAndDashes      = "[.\\-]".r
  private val pascalCaseBoundary = "([^A-Z_])([A-Z])".r

  private def normaliseVendor(vendor: String): String =
    dotsAndDashes.replaceAllIn(vendor, "_").toLowerCase(Locale.ROOT)

  private def normaliseName(name: String): String =
    pascalCaseBoundary.replaceAllIn(dotsAndDashes.replaceAllIn(name, "_"), "$1_$2").toLowerCase(Locale.ROOT)

  private def middleHash(
    normalised: String,
    raw: String,
    length: Int
  ): String = {
    val keep  = length - HashLength - 2
    val start = keep - keep / 2
    val end   = keep / 2
    s"${normalised.take(start)}_${md5Prefix(raw)}_${normalised.takeRight(end)}"
  }

  private val hexDigits = "0123456789abcdef".toCharArray

  // The first HashLength lowercase hex characters of the MD5, encoding only the bytes they need
  private def md5Prefix(s: String): String = {
    val digest = MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8))
    val chars  = new Array[Char](HashLength)
    (0 until HashLength / 2).foreach { i =>
      chars(2 * i)     = hexDigits((digest(i) >> 4) & 0xf)
      chars(2 * i + 1) = hexDigits(digest(i) & 0xf)
    }
    new String(chars)
  }

  private def keyToSubVersion(key: SchemaKey): SchemaSubVersion = (key.version.revision, key.version.addition)

  private[transform] def forUnstructEvent(schemaKey: SchemaKey): TabledEntity =
    TabledEntity(UnstructEvent, schemaKey.vendor, schemaKey.name, schemaKey.version.model)

  private[transform] def forContext(schemaKey: SchemaKey): TabledEntity =
    TabledEntity(Context, schemaKey.vendor, schemaKey.name, schemaKey.version.model)
}
