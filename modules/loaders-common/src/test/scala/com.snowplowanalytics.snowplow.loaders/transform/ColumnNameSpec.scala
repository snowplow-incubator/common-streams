/*
 * Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import com.snowplowanalytics.snowplow.analytics.scalasdk.{Data, SnowplowEvent}
import org.scalacheck.{Gen, Prop}
import org.specs2.{ScalaCheck, Specification}

class ColumnNameSpec extends Specification with ScalaCheck {
  import ColumnNameSpec._

  def is = s2"""
  TabledEntity.columnName should
    Give the golden names for the golden inputs $golden
    Never give a name longer than maxLength $withinLimit
    Give the Analytics SDK's transformSchema name, with the chosen prefix, when that is within maxLength $sdkName
    Hash the raw vendor, so vendors which normalise to the same text get different names when shortened $rawHash
    End with the model $endsWithModel
    Reject a maxLength below MinMaxLength $rejectSmallLimit
  """

  def golden =
    Golden
      .map { case (te, maxLength, useShortPrefixes, expected) =>
        TabledEntity.columnName(te, maxLength, useShortPrefixes) must beEqualTo(expected)
      }
      .reduce(_ and _)

  def withinLimit =
    Prop.forAll(genTabledEntity, genMaxLength, Gen.oneOf(true, false)) { (te, maxLength, useShortPrefixes) =>
      TabledEntity.columnName(te, maxLength, useShortPrefixes).length must beLessThanOrEqualTo(maxLength)
    }

  def sdkName =
    Prop.forAll(genTabledEntity, genMaxLength, Gen.oneOf(true, false)) { (te, maxLength, useShortPrefixes) =>
      val expected = sdkColumnName(te, useShortPrefixes)
      val result   = TabledEntity.columnName(te, maxLength, useShortPrefixes)
      if (expected.length <= maxLength) result must beEqualTo(expected)
      else result.length must beEqualTo(maxLength)
    }

  def rawHash = {
    val gen = for {
      vendor <- genVendor.suchThat(v => v.length > TabledEntity.MinHashedLength && v.exists(_.isLower))
      name <- genName
      model <- genModel
      entityType <- Gen.oneOf(TabledEntity.UnstructEvent, TabledEntity.Context)
      useShortPrefixes <- Gen.oneOf(true, false)
    } yield (TabledEntity(entityType, vendor, name, model), useShortPrefixes)

    Prop.forAll(gen) { case (te, useShortPrefixes) =>
      val other = te.copy(vendor = te.vendor.toUpperCase)
      val name  = TabledEntity.columnName(te, maxLength = 63, useShortPrefixes)
      if (sdkColumnName(te, useShortPrefixes).length > 63)
        name must not(beEqualTo(TabledEntity.columnName(other, maxLength = 63, useShortPrefixes)))
      else
        name must beEqualTo(TabledEntity.columnName(other, maxLength = 63, useShortPrefixes))
    }
  }

  def endsWithModel =
    Prop.forAll(genTabledEntity, genMaxLength, Gen.oneOf(true, false)) { (te, maxLength, useShortPrefixes) =>
      TabledEntity.columnName(te, maxLength, useShortPrefixes) must endWith(s"_${te.model}")
    }

  def rejectSmallLimit =
    TabledEntity.columnName(Golden.head._1, TabledEntity.MinMaxLength - 1, false) must throwAn[IllegalArgumentException]
}

object ColumnNameSpec {

  /**
   * THESE EXPECTATIONS MUST NEVER CHANGE.
   *
   * Loaders have created columns with these names in customers' tables. A change to any of them
   * means a loader would create a duplicate column for an existing schema family.
   *
   * With valid Iglu keys, which cap vendor and name at 128 characters each, a limit of 255 never
   * needs the name hashed, so the vendor-and-name examples are at 63.
   */
  val Golden: List[(TabledEntity, Int, Boolean, String)] = List(
    (
      TabledEntity(TabledEntity.Context, "com.snowplowanalytics.snowplow", "web_page", 1),
      63,
      false,
      "contexts_com_snowplowanalytics_snowplow_web_page_1"
    ),
    (
      TabledEntity(TabledEntity.Context, "com.snowplowanalytics.snowplow", "web_page", 1),
      63,
      true,
      "ctx_com_snowplowanalytics_snowplow_web_page_1"
    ),
    (
      TabledEntity(TabledEntity.Context, "com.snowplowanalytics.snowplow", "web_page", 1),
      255,
      false,
      "contexts_com_snowplowanalytics_snowplow_web_page_1"
    ),
    (
      TabledEntity(TabledEntity.Context, "com.snowplowanalytics.snowplow", "web_page", 1),
      255,
      true,
      "ctx_com_snowplowanalytics_snowplow_web_page_1"
    ),
    (
      TabledEntity(TabledEntity.UnstructEvent, "com.snowplowanalytics.snowplow", "application_error", 1),
      63,
      false,
      "unstruct_event_com_snowp_a45059fa__snowplow_application_error_1"
    ),
    (
      TabledEntity(TabledEntity.UnstructEvent, "com.snowplowanalytics.snowplow", "application_error", 1),
      63,
      true,
      "ue_com_snowplowanalytics_snowplow_application_error_1"
    ),
    (
      TabledEntity(TabledEntity.UnstructEvent, "com.snowplowanalytics.snowplow", "application_error", 1),
      255,
      false,
      "unstruct_event_com_snowplowanalytics_snowplow_application_error_1"
    ),
    (
      TabledEntity(TabledEntity.UnstructEvent, "com.snowplowanalytics.snowplow", "application_error", 1),
      255,
      true,
      "ue_com_snowplowanalytics_snowplow_application_error_1"
    ),
    (
      TabledEntity(TabledEntity.Context, "com.snowplowanalytics.snowplow.media", "ad_break_start_event", 1),
      63,
      false,
      "contexts_com_snowplo_36c6eb80_plow_media_ad_break_start_event_1"
    ),
    (
      TabledEntity(TabledEntity.Context, "com.snowplowanalytics.snowplow.media", "ad_break_start_event", 1),
      63,
      true,
      "ctx_com_snowplowanalytics_snowplow_media_ad_break_start_event_1"
    ),
    (
      TabledEntity(TabledEntity.Context, "com.snowplowanalytics.snowplow.media", "ad_break_start_event", 1),
      255,
      false,
      "contexts_com_snowplowanalytics_snowplow_media_ad_break_start_event_1"
    ),
    (
      TabledEntity(TabledEntity.Context, "com.snowplowanalytics.snowplow.media", "ad_break_start_event", 1),
      255,
      true,
      "ctx_com_snowplowanalytics_snowplow_media_ad_break_start_event_1"
    ),
    (
      TabledEntity(TabledEntity.UnstructEvent, "com.snowplowanalytics.snowplow.ecommerce", "snowplow_ecommerce_action", 1),
      63,
      false,
      "unstruct_event_com_s_6f443685_merce_snowplow_ecommerce_action_1"
    ),
    (
      TabledEntity(TabledEntity.UnstructEvent, "com.snowplowanalytics.snowplow.ecommerce", "snowplow_ecommerce_action", 1),
      63,
      true,
      "ue_com_snowplo_6f443685_w_ecommerce_snowplow_ecommerce_action_1"
    ),
    (
      TabledEntity(TabledEntity.UnstructEvent, "com.snowplowanalytics.snowplow.ecommerce", "snowplow_ecommerce_action", 1),
      255,
      false,
      "unstruct_event_com_snowplowanalytics_snowplow_ecommerce_snowplow_ecommerce_action_1"
    ),
    (
      TabledEntity(TabledEntity.UnstructEvent, "com.snowplowanalytics.snowplow.ecommerce", "snowplow_ecommerce_action", 1),
      255,
      true,
      "ue_com_snowplowanalytics_snowplow_ecommerce_snowplow_ecommerce_action_1"
    ),
    (TabledEntity(TabledEntity.Context, "com.acme", "CheckoutStep-v2", 10), 63, false, "contexts_com_acme_checkout_step_v2_10"),
    (TabledEntity(TabledEntity.Context, "com.acme", "CheckoutStep-v2", 10), 63, true, "ctx_com_acme_checkout_step_v2_10"),
    (TabledEntity(TabledEntity.Context, "com.acme", "CheckoutStep-v2", 10), 255, false, "contexts_com_acme_checkout_step_v2_10"),
    (TabledEntity(TabledEntity.Context, "com.acme", "CheckoutStep-v2", 10), 255, true, "ctx_com_acme_checkout_step_v2_10"),
    (
      TabledEntity(TabledEntity.UnstructEvent, "com.acme", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxPurchaseCompletedWithManyDetails", 3),
      63,
      false,
      "unstruct_event_com_acme_xxxxxxxxxxxxxx_334a4bec__many_details_3"
    ),
    (
      TabledEntity(TabledEntity.UnstructEvent, "com.acme", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxPurchaseCompletedWithManyDetails", 3),
      63,
      true,
      "ue_com_acme_xxxxxxxxxxxxxxxxxxxx_334a4bec_d_with_many_details_3"
    ),
    (
      TabledEntity(TabledEntity.UnstructEvent, "com.acme", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxPurchaseCompletedWithManyDetails", 3),
      255,
      false,
      "unstruct_event_com_acme_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx_purchase_completed_with_many_details_3"
    ),
    (
      TabledEntity(TabledEntity.UnstructEvent, "com.acme", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxPurchaseCompletedWithManyDetails", 3),
      255,
      true,
      "ue_com_acme_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx_purchase_completed_with_many_details_3"
    ),
    (
      TabledEntity(
        TabledEntity.Context,
        "com.acme-corporation.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.Analytics",
        "VeryLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchema",
        2
      ),
      63,
      false,
      "contexts_c_57acb64e_s_very_long_schem_244741bb_ma_long_schema_2"
    ),
    (
      TabledEntity(
        TabledEntity.Context,
        "com.acme-corporation.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.Analytics",
        "VeryLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchema",
        2
      ),
      63,
      true,
      "ctx_c_57acb64e_s_very_long_schema__244741bb_chema_long_schema_2"
    ),
    (
      TabledEntity(
        TabledEntity.Context,
        "com.acme-corporation.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.Analytics",
        "VeryLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchema",
        2
      ),
      255,
      false,
      "contexts_com_acme_corporation_xxxxxxxxxxxxxxxxxxxxxx_57acb64e_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx_analytics_very_long_schema_long_schema_long_schema_long_schema_long_schema_long_schema_long_schema_long_schema_long_schema_long_schema_long_schema_long_schema_2"
    ),
    (
      TabledEntity(
        TabledEntity.Context,
        "com.acme-corporation.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.Analytics",
        "VeryLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchemaLongSchema",
        2
      ),
      255,
      true,
      "ctx_com_acme_corporation_xxxxxxxxxxxxxxxxxxxxxxxx_57acb64e_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx_analytics_very_long_schema_long_schema_long_schema_long_schema_long_schema_long_schema_long_schema_long_schema_long_schema_long_schema_long_schema_long_schema_2"
    )
  )

  private def sdkColumnName(te: TabledEntity, useShortPrefixes: Boolean): String =
    te.entityType match {
      case TabledEntity.UnstructEvent =>
        val name = SnowplowEvent.transformSchema(Data.UnstructEvent, te.vendor, te.schemaName, te.model)
        if (useShortPrefixes) "ue_" + name.stripPrefix("unstruct_event_") else name
      case TabledEntity.Context =>
        val name = SnowplowEvent.transformSchema(Data.Contexts(Data.CustomContexts), te.vendor, te.schemaName, te.model)
        if (useShortPrefixes) "ctx_" + name.stripPrefix("contexts_") else name
    }

  // Iglu's `SchemaKey.schemaUriRegex`
  private val vendorChars = Gen.oneOf(('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ "-_.")
  private val nameChars   = Gen.oneOf(('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ "-_")

  private def genComponent(chars: Gen[Char]): Gen[String] =
    Gen.oneOf(Gen.choose(1, 30), Gen.choose(1, 128)).flatMap(n => Gen.listOfN(n, chars)).map(_.mkString)

  val genVendor: Gen[String] = genComponent(vendorChars)
  val genName: Gen[String]   = genComponent(nameChars)
  val genModel: Gen[Int]     = Gen.oneOf(Gen.choose(0, 20), Gen.choose(0, Int.MaxValue))

  val genMaxLength: Gen[Int] = Gen.oneOf(Gen.const(63), Gen.const(255), Gen.choose(TabledEntity.MinMaxLength, 300))

  val genTabledEntity: Gen[TabledEntity] = for {
    entityType <- Gen.oneOf(TabledEntity.UnstructEvent, TabledEntity.Context)
    vendor <- genVendor
    name <- genName
    model <- genModel
  } yield TabledEntity(entityType, vendor, name, model)
}
