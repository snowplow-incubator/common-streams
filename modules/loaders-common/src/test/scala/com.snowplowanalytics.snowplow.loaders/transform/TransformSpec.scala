/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import org.specs2.Specification
import io.circe.Json
import io.circe.literal._

import com.snowplowanalytics.iglu.schemaddl.parquet.Caster.NamedValue
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.snowplow.badrows.{Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Event, SnowplowEvent}

import java.util.UUID
import java.time.Instant

class TransformSpec extends Specification {
  import TransformSpec._

  def is = s2"""
  Transform.transformEventUnstructured should
    Transform a minimal valid event $e1
    Transform a valid event with each different type of atomic field $e2
    Create a failed event if a currency field cannot be cast to a decimal $e3
    Create additional columns for unstruct events $e4
    Create additional columns for contexts, using different columns for different schemas $e5
    Create additional columns for contexts, using same column when schemas have same major version $e6
  """

  def e1 = {
    val event = Event.minimal(testEventId, testTimestamp, "0.0.0", "0.0.0")

    val result = Transform.transformEventUnstructured(badProcessor, TestCaster, TestCirceFolder, event)

    val expected = List(
      NamedValue("event_id", Json.fromString(testEventId.toString)),
      NamedValue("collector_tstamp", Json.fromString(testTimestamp.toString)),
      NamedValue("geo_region", Json.Null)
    )

    result must beRight { namedValues: List[NamedValue[Json]] =>
      expected
        .map { e =>
          namedValues must contain(e).exactly(1.times)
        }
        .reduce(_ and _)
    }
  }

  def e2 = {
    val event = Event
      .minimal(testEventId, testTimestamp, "0.0.0", "0.0.0")
      .copy(
        app_id              = Some("myapp"),
        dvce_created_tstamp = Some(testTimestamp),
        txn_id              = Some(42),
        geo_latitude        = Some(1.234),
        dvce_ismobile       = Some(true),
        tr_total            = Some(12.34)
      )

    val result = Transform.transformEventUnstructured(badProcessor, TestCaster, TestCirceFolder, event)

    val expected = List(
      NamedValue("app_id", Json.fromString("myapp")),
      NamedValue("dvce_created_tstamp", Json.fromString(testTimestamp.toString)),
      NamedValue("txn_id", Json.fromInt(42)),
      NamedValue("geo_latitude", Json.fromDoubleOrNull(1.234)),
      NamedValue("dvce_ismobile", Json.True),
      NamedValue("tr_total", Json.fromDoubleOrNull(12.34))
    )

    result must beRight { namedValues: List[NamedValue[Json]] =>
      expected
        .map { e =>
          namedValues must contain(e).exactly(1.times)
        }
        .reduce(_ and _)
    }
  }

  def e3 = {
    val event = Event
      .minimal(testEventId, testTimestamp, "0.0.0", "0.0.0")
      .copy(
        tr_total = Some(12.3456) // Too many decimal points
      )

    val result = Transform.transformEventUnstructured(badProcessor, TestCaster, TestCirceFolder, event)

    result must beLeft
  }

  def e4 = {
    val data = json"""
    {
      "my_string": "abc",
      "my_int":     42,
      "my_decimal": 1.23,
      "my_boolean": true,
      "my_array":   [1,2,3],
      "my_object":  {"abc": "xyz"},
      "my_null":    null
    }
    """

    val sdj = SelfDescribingData[Json](testSchemaKey701, data)
    val event = Event
      .minimal(testEventId, testTimestamp, "0.0.0", "0.0.0")
      .copy(unstruct_event = SnowplowEvent.UnstructEvent(Some(sdj)))

    val result = Transform.transformEventUnstructured(badProcessor, TestCaster, TestCirceFolder, event)

    val expected = NamedValue("unstruct_event_com_example_my_schema_7", data)

    result must beRight { namedValues: List[NamedValue[Json]] =>
      namedValues must contain(expected).exactly(1.times)
    }
  }

  def e5 = {
    val data1 = json"""
    {
      "my_string": "abc",
      "my_int":     42
    }
    """

    val data2 = json"""
    {
      "my_string": "xyz",
      "my_int":     123
    }
    """

    val contexts = SnowplowEvent.Contexts(
      List(
        SelfDescribingData[Json](testSchemaKey701, data1),
        SelfDescribingData[Json](testSchemaKey801, data2)
      )
    )

    val event = Event
      .minimal(testEventId, testTimestamp, "0.0.0", "0.0.0")
      .copy(contexts = contexts)

    val result = Transform.transformEventUnstructured(badProcessor, TestCaster, TestCirceFolder, event)

    val expected = List(
      NamedValue("contexts_com_example_my_schema_7", Json.fromValues(List(data1))),
      NamedValue("contexts_com_example_my_schema_8", Json.fromValues(List(data2)))
    )

    result must beRight { namedValues: List[NamedValue[Json]] =>
      expected
        .map { e =>
          namedValues must contain(e).exactly(1.times)
        }
        .reduce(_ and _)
    }
  }

  def e6 = {
    val data1 = json"""
    {
      "my_string": "abc",
      "my_int":     42
    }
    """

    val data2 = json"""
    {
      "my_string": "xyz",
      "my_int":     123
    }
    """

    val contexts = SnowplowEvent.Contexts(
      List(
        SelfDescribingData[Json](testSchemaKey701, data1),
        SelfDescribingData[Json](testSchemaKey702, data2)
      )
    )

    val event = Event
      .minimal(testEventId, testTimestamp, "0.0.0", "0.0.0")
      .copy(contexts = contexts)

    val result = Transform.transformEventUnstructured(badProcessor, TestCaster, TestCirceFolder, event)

    val expected = NamedValue("contexts_com_example_my_schema_7", Json.fromValues(List(data1, data2)))

    result must beRight { namedValues: List[NamedValue[Json]] =>
      namedValues must contain(expected).exactly(1.times)
    }
  }

}

object TransformSpec {

  val testEventId   = UUID.randomUUID
  val testTimestamp = Instant.now

  val badProcessor = BadRowProcessor("snowflake-loader", "0.0.0")

  val testSchemaKey701 = SchemaKey.fromUri("iglu:com.example/mySchema/jsonschema/7-0-1").toOption.get
  val testSchemaKey702 = SchemaKey.fromUri("iglu:com.example/mySchema/jsonschema/7-0-2").toOption.get
  val testSchemaKey801 = SchemaKey.fromUri("iglu:com.example/mySchema/jsonschema/8-0-1").toOption.get
}
