/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import org.specs2.Specification
import cats.data.NonEmptyVector
import io.circe._
import io.circe.literal._
import io.circe.syntax._

import com.snowplowanalytics.iglu.schemaddl.parquet.Caster.NamedValue
import com.snowplowanalytics.iglu.schemaddl.parquet.{Caster, Type}
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SelfDescribingData}
import com.snowplowanalytics.snowplow.badrows.{Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Event, SnowplowEvent}
import com.snowplowanalytics.snowplow.eventgen.runGen
import com.snowplowanalytics.snowplow.eventgen.enrich.{SdkEvent => GenSdkEvent}
import com.snowplowanalytics.snowplow.eventgen.protocol.event._
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}

import java.util.UUID
import java.time.{Instant, LocalDate}
import scala.collection.mutable.ListBuffer
import scala.util.Random

class TransformUnstructuredSpec extends Specification {
  import TransformUnstructuredSpec._

  def is = s2"""
  Transform.transformEventUnstructured should
    Transform a minimal valid event $e1
    Transform a valid event with each different type of atomic field $e2
    Create a failed event if a currency field cannot be cast to a decimal due to rounding $e3_1
    Create a failed event if a currency field cannot be cast to a decimal due to high precision $e3_2
    Create additional columns for unstruct events $e4
    Create additional columns for contexts, using different columns for different schemas $e5
    Create additional columns for contexts, using same column when schemas have same major version $e6
    Create additional columns for contexts, using the anything-a schema with all possible types of JSON $e7
    Transform values of event fields correctly $e8
    Transform types of event fields correctly $e9
    Put custom and derived contexts of the same schema family in one column, custom first $e10
    Create separate columns for custom and derived contexts of different schema families $e11
    Call entityValue once per entity column, with the whole gathered JSON $e12
    Not call entityValue for skipped entities $e13
    Name entity columns with TabledEntity.columnName, honouring maxColumnNameLength and useShortPrefixes $e14
    Put contexts of different schema families with the same column name in one column, in event order $e15
  """

  def e1 = {
    val event = Event.minimal(testEventId, testTimestamp, "0.0.0", "0.0.0")

    val result =
      Transform.transformEventUnstructured(badProcessor, TestCaster, _.foldWith(TestCirceFolder), event, schemasToSkip, Int.MaxValue, false)

    val expected = List(
      NamedValue("event_id", Json.fromString(testEventId.toString)),
      NamedValue("collector_tstamp", Json.fromString(testTimestamp.toString)),
      NamedValue("geo_region", Json.Null)
    )

    result must beRight { namedValues: Vector[NamedValue[Json]] =>
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

    val result =
      Transform.transformEventUnstructured(badProcessor, TestCaster, _.foldWith(TestCirceFolder), event, schemasToSkip, Int.MaxValue, false)

    val expected = List(
      NamedValue("app_id", Json.fromString("myapp")),
      NamedValue("dvce_created_tstamp", Json.fromString(testTimestamp.toString)),
      NamedValue("txn_id", Json.fromInt(42)),
      NamedValue("geo_latitude", Json.fromDoubleOrNull(1.234)),
      NamedValue("dvce_ismobile", Json.True),
      NamedValue("tr_total", Json.fromDoubleOrNull(12.34))
    )

    result must beRight { namedValues: Vector[NamedValue[Json]] =>
      expected
        .map { e =>
          namedValues must contain(e).exactly(1.times)
        }
        .reduce(_ and _)
    }
  }

  def e3_1 = {
    val event = Event
      .minimal(testEventId, testTimestamp, "0.0.0", "0.0.0")
      .copy(
        tr_total = Some(12.3456) // Too many decimal points
      )

    val result =
      Transform.transformEventUnstructured(badProcessor, TestCaster, _.foldWith(TestCirceFolder), event, schemasToSkip, Int.MaxValue, false)

    result must beLeft
  }

  def e3_2 = {
    val event = Event
      .minimal(testEventId, testTimestamp, "0.0.0", "0.0.0")
      .copy(
        tr_total = Some(12345678987654321.34) // Too high precision
      )

    val result =
      Transform.transformEventUnstructured(badProcessor, TestCaster, _.foldWith(TestCirceFolder), event, schemasToSkip, Int.MaxValue, false)

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

    val result =
      Transform.transformEventUnstructured(badProcessor, TestCaster, _.foldWith(TestCirceFolder), event, schemasToSkip, Int.MaxValue, false)

    val expected = NamedValue("unstruct_event_com_example_my_schema_7", data)

    result must beRight { namedValues: Vector[NamedValue[Json]] =>
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

    val result =
      Transform.transformEventUnstructured(badProcessor, TestCaster, _.foldWith(TestCirceFolder), event, schemasToSkip, Int.MaxValue, false)

    val expected = List(
      NamedValue(
        name = "contexts_com_example_my_schema_7",
        value = json"""
              [{
                "_schema_version" : "7-0-1",
                "my_string": "abc",
                "my_int":     42
              }]
              """
      ),
      NamedValue(
        name = "contexts_com_example_my_schema_8",
        value = json"""
              [{
                "_schema_version" : "8-0-1",
                "my_string": "xyz",
                "my_int":     123 
              }]
              """
      )
    )

    result must beRight { namedValues: Vector[NamedValue[Json]] =>
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

    val result =
      Transform.transformEventUnstructured(badProcessor, TestCaster, _.foldWith(TestCirceFolder), event, schemasToSkip, Int.MaxValue, false)

    val expected = NamedValue(
      name = "contexts_com_example_my_schema_7",
      value = json"""
            [{
              "_schema_version" : "7-0-1",
              "my_string": "abc",
              "my_int":     42
            },
            {
              "_schema_version" : "7-0-2",
              "my_string": "xyz",
              "my_int":     123 
            }]
            """
    )

    result must beRight { namedValues: Vector[NamedValue[Json]] =>
      namedValues must contain(expected).exactly(1.times)
    }
  }

  def e7 = {

    val contexts = SnowplowEvent.Contexts(
      List(
        SelfDescribingData[Json](anythingASchemaKey, json""""xyz""""), // string value
        SelfDescribingData[Json](anythingASchemaKey, json"""42"""), // numeric value
        SelfDescribingData[Json](anythingASchemaKey, json"""[1, 2, 3]"""), // array value
        SelfDescribingData[Json](anythingASchemaKey, json"""{"abc": "xyz"}"""), // object value
        SelfDescribingData[Json](anythingASchemaKey, json"""null""") // null value
      )
    )

    val event = Event
      .minimal(testEventId, testTimestamp, "0.0.0", "0.0.0")
      .copy(contexts = contexts)

    val result =
      Transform.transformEventUnstructured(badProcessor, TestCaster, _.foldWith(TestCirceFolder), event, schemasToSkip, Int.MaxValue, false)

    val expected = NamedValue(
      name = "contexts_com_snowplowanalytics_iglu_anything_a_1",
      value = json"""[
          "xyz",
          42,
          [1, 2, 3],
          {"_schema_version": "1-0-0", "abc": "xyz"},
          null
        ]"""
    )

    result must beRight { namedValues: Vector[NamedValue[Json]] =>
      namedValues must contain(expected).exactly(1.times)
    }
  }

  def e8 =
    forall(genEvents(100)) { event =>
      val eventMap = getParams(event)
      val result =
        Transform
          .transformEventUnstructured(badProcessor, TestCaster, _.foldWith(TestCirceFolder), event, schemasToSkip, Int.MaxValue, false)
          .toOption
          .get
      forall(result)(c => c.value must beEqualTo(getFieldValueFromEvent(eventMap, c.name)))
    }

  def e9 =
    forall(genEvents(100)) { event =>
      val eventMap = getParams(event)
      val result =
        Transform
          .transformEventUnstructured(badProcessor, typeCaster, _.foldWith(typeCirceFolder), event, schemasToSkip, Int.MaxValue, false)
          .toOption
          .get
      forall(result)(c => c.value must beEqualTo(getFieldTypeFromEvent(eventMap, c.name)))
    }

  def e10 = {
    val contexts        = SnowplowEvent.Contexts(List(SelfDescribingData[Json](acmeFooKey100, json"""{"a": 1}""")))
    val derivedContexts = SnowplowEvent.Contexts(List(SelfDescribingData[Json](acmeFooKey101, json"""{"a": 2}""")))

    val event = Event
      .minimal(testEventId, testTimestamp, "0.0.0", "0.0.0")
      .copy(contexts = contexts, derived_contexts = derivedContexts)

    val result =
      Transform.transformEventUnstructured(badProcessor, TestCaster, _.foldWith(TestCirceFolder), event, schemasToSkip, Int.MaxValue, false)

    val expected = NamedValue(
      name  = "contexts_com_acme_foo_1",
      value = json"""[{"_schema_version": "1-0-0", "a": 1}, {"_schema_version": "1-0-1", "a": 2}]"""
    )

    result must beRight { namedValues: Vector[NamedValue[Json]] =>
      namedValues must contain(expected).exactly(1.times)
    }
  }

  def e11 = {
    val contexts        = SnowplowEvent.Contexts(List(SelfDescribingData[Json](testSchemaKey701, json"""{"my_string": "abc"}""")))
    val derivedContexts = SnowplowEvent.Contexts(List(SelfDescribingData[Json](testSchemaKey801, json"""{"my_string": "xyz"}""")))

    val event = Event
      .minimal(testEventId, testTimestamp, "0.0.0", "0.0.0")
      .copy(contexts = contexts, derived_contexts = derivedContexts)

    val result =
      Transform.transformEventUnstructured(badProcessor, TestCaster, _.foldWith(TestCirceFolder), event, schemasToSkip, Int.MaxValue, false)

    val expected = List(
      NamedValue(
        name  = "contexts_com_example_my_schema_7",
        value = json"""[{"_schema_version": "7-0-1", "my_string": "abc"}]"""
      ),
      NamedValue(
        name  = "contexts_com_example_my_schema_8",
        value = json"""[{"_schema_version": "8-0-1", "my_string": "xyz"}]"""
      )
    )

    result must beRight { namedValues: Vector[NamedValue[Json]] =>
      expected
        .map { e =>
          namedValues must contain(e).exactly(1.times)
        }
        .reduce(_ and _)
    }
  }

  def e12 = {
    val calls = ListBuffer.empty[Json]

    val result = Transform.transformEventUnstructured(
      badProcessor,
      TestCaster,
      { json: Json => calls += json; Json.fromInt(calls.size) },
      eventWithEveryKindOfEntity,
      schemasToSkip,
      Int.MaxValue,
      false
    )

    val entityColumns = result.toOption.get.filterNot(nv => AtomicFields.static.exists(_.name == nv.name))

    val expectedColumns = Map[String, Json](
      "contexts_com_acme_foo_1" -> json"""[{"_schema_version": "1-0-0", "a": 1}, {"_schema_version": "1-0-1", "a": 2}]""",
      "contexts_com_example_my_schema_7" -> json"""[{"_schema_version": "7-0-1", "my_string": "abc"}]""",
      "unstruct_event_com_example_my_event_1" -> json"""{"my_string": "xyz"}"""
    )

    // entityValue returns the call's position, so each column's value identifies the JSON it was called with
    val columnsByCall = entityColumns.map(nv => nv.name -> calls(nv.value.asNumber.flatMap(_.toInt).get - 1)).toMap

    (calls.size, columnsByCall) must beEqualTo((expectedColumns.size, expectedColumns))
  }

  def e13 = {
    val calls = ListBuffer.empty[Json]

    val result = Transform.transformEventUnstructured(
      badProcessor,
      TestCaster,
      { json: Json => calls += json; json },
      eventWithEveryKindOfEntity,
      List(SchemaCriterion("com.acme", "foo", "jsonschema", 1)),
      Int.MaxValue,
      false
    )

    (result must beRight) and
      (calls.toList must containTheSameElementsAs(
        List(
          json"""[{"_schema_version": "7-0-1", "my_string": "abc"}]""",
          json"""{"my_string": "xyz"}"""
        )
      ))
  }

  def e14 = {
    val ecommerceKey =
      SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow.ecommerce/snowplow_ecommerce_action/jsonschema/1-0-2").toOption.get

    val event = eventWithEveryKindOfEntity.copy(
      unstruct_event = SnowplowEvent.UnstructEvent(Some(SelfDescribingData[Json](ecommerceKey, json"""{"type": "add_to_cart"}""")))
    )

    val result = Transform.transformEventUnstructured(badProcessor, TestCaster, identity, event, schemasToSkip, 63, true)

    val expected = List(
      NamedValue(
        name  = "ctx_com_acme_foo_1",
        value = json"""[{"_schema_version": "1-0-0", "a": 1}, {"_schema_version": "1-0-1", "a": 2}]"""
      ),
      NamedValue(
        name  = "ctx_com_example_my_schema_7",
        value = json"""[{"_schema_version": "7-0-1", "my_string": "abc"}]"""
      ),
      NamedValue(
        name  = "ue_com_snowplo_6f443685_w_ecommerce_snowplow_ecommerce_action_1",
        value = json"""{"type": "add_to_cart"}"""
      )
    )

    result must beRight { namedValues: Vector[NamedValue[Json]] =>
      expected
        .map { e =>
          namedValues must contain(e).exactly(1.times)
        }
        .reduce(_ and _)
    }
  }

  def e15 = {
    val dotKey        = SchemaKey.fromUri("iglu:com.acme_foo/bar/jsonschema/1-0-0").toOption.get
    val underscoreKey = SchemaKey.fromUri("iglu:com.acme/foo_bar/jsonschema/1-0-0").toOption.get

    val event = Event
      .minimal(testEventId, testTimestamp, "0.0.0", "0.0.0")
      .copy(
        contexts         = SnowplowEvent.Contexts(List(SelfDescribingData[Json](dotKey, json"""{"a": 1}"""))),
        derived_contexts = SnowplowEvent.Contexts(List(SelfDescribingData[Json](underscoreKey, json"""{"a": 2}""")))
      )

    val result = Transform.transformEventUnstructured(badProcessor, TestCaster, identity, event, schemasToSkip, Int.MaxValue, false)

    val expected = NamedValue(
      name  = "contexts_com_acme_foo_bar_1",
      value = json"""[{"_schema_version": "1-0-0", "a": 1}, {"_schema_version": "1-0-0", "a": 2}]"""
    )

    result must beRight { namedValues: Vector[NamedValue[Json]] =>
      namedValues must contain(expected).exactly(1.times)
    }
  }

}

object TransformUnstructuredSpec {

  val testEventId   = UUID.randomUUID
  val testTimestamp = Instant.now
  val schemasToSkip = List.empty

  val badProcessor = BadRowProcessor("snowflake-loader", "0.0.0")

  val testSchemaKey701   = SchemaKey.fromUri("iglu:com.example/mySchema/jsonschema/7-0-1").toOption.get
  val testSchemaKey702   = SchemaKey.fromUri("iglu:com.example/mySchema/jsonschema/7-0-2").toOption.get
  val testSchemaKey801   = SchemaKey.fromUri("iglu:com.example/mySchema/jsonschema/8-0-1").toOption.get
  val anythingASchemaKey = SchemaKey.fromUri("iglu:com.snowplowanalytics.iglu/anything-a/jsonschema/1-0-0").toOption.get
  val acmeFooKey100      = SchemaKey.fromUri("iglu:com.acme/foo/jsonschema/1-0-0").toOption.get
  val acmeFooKey101      = SchemaKey.fromUri("iglu:com.acme/foo/jsonschema/1-0-1").toOption.get
  val myEventKey100      = SchemaKey.fromUri("iglu:com.example/myEvent/jsonschema/1-0-0").toOption.get

  // A context family in both custom and derived contexts, a context family only in custom contexts, and an unstruct event
  val eventWithEveryKindOfEntity: Event =
    Event
      .minimal(testEventId, testTimestamp, "0.0.0", "0.0.0")
      .copy(
        contexts = SnowplowEvent.Contexts(
          List(
            SelfDescribingData[Json](acmeFooKey100, json"""{"a": 1}"""),
            SelfDescribingData[Json](testSchemaKey701, json"""{"my_string": "abc"}""")
          )
        ),
        derived_contexts = SnowplowEvent.Contexts(List(SelfDescribingData[Json](acmeFooKey101, json"""{"a": 2}"""))),
        unstruct_event   = SnowplowEvent.UnstructEvent(Some(SelfDescribingData[Json](myEventKey100, json"""{"my_string": "xyz"}""")))
      )

  val typeCaster = new Caster[String] {
    override def nullValue: String                                                  = "null"
    override def jsonValue(v: Json): String                                         = "json"
    override def stringValue(v: String): String                                     = "string"
    override def booleanValue(v: Boolean): String                                   = "boolean"
    override def intValue(v: Int): String                                           = "int"
    override def longValue(v: Long): String                                         = "long"
    override def doubleValue(v: Double): String                                     = "double"
    override def decimalValue(unscaled: BigInt, details: Type.Decimal): String      = "double"
    override def timestampValue(v: Instant): String                                 = "timestamp"
    override def dateValue(v: LocalDate): String                                    = "date"
    override def arrayValue(vs: Vector[String]): String                             = "array"
    override def structValue(vs: NonEmptyVector[Caster.NamedValue[String]]): String = "struct"
  }

  val typeCirceFolder = new Json.Folder[String] {
    override def onNull: String                       = "null"
    override def onBoolean(value: Boolean): String    = "boolean"
    override def onNumber(value: JsonNumber): String  = "number"
    override def onString(value: String): String      = "string"
    override def onArray(value: Vector[Json]): String = "array"
    override def onObject(value: JsonObject): String  = "object"
  }

  def genEvents(n: Int): List[Event] =
    (1 to n).toList.flatMap(_ =>
      runGen(
        GenSdkEvent.gen(
          eventPerPayloadMin  = 1,
          eventPerPayloadMax  = 1,
          now                 = Instant.now,
          frequencies         = EventFrequencies(1, 1, 1, 1, 1, 1, UnstructEventFrequencies(1, 1, 1)),
          generateEnrichments = true
        ),
        new Random()
      )
    )

  def getParams(event: Event): Map[String, Any] = {
    val values = event.productIterator.filter {
      case _: Contexts | _: UnstructEvent => false
      case _                              => true
    }
    // We can't use 'Product.productElementNames' method to get field names
    // because that method is introduced in Scala 2.13. Since we want to run
    // tests with Scala 2.12 as well, we can't use that method.
    (AtomicFields.static.map(_.name) zip values.toList).toMap +
      ("contexts" -> event.contexts) +
      ("unstruct_event" -> event.unstruct_event)
  }

  def getFieldValueFromEvent(eventMap: Map[String, Any], fieldName: String): Json =
    if (fieldName.startsWith("contexts_"))
      eventMap("contexts").asInstanceOf[Contexts].toShreddedJson(fieldName)
    else if (fieldName.startsWith("unstruct_"))
      eventMap("unstruct_event").asInstanceOf[UnstructEvent].toShreddedJson.get._2
    else
      eventMap(fieldName) match {
        case Some(a) => anyToJson(a)
        case None    => Json.Null
        case a       => anyToJson(a)
      }

  def getFieldTypeFromEvent(eventMap: Map[String, Any], fieldName: String): String =
    if (fieldName.startsWith("contexts_")) "array"
    else if (fieldName.startsWith("unstruct_")) "object"
    else
      eventMap(fieldName) match {
        case Some(a) => anyToType(a)
        case None    => "null"
        case a       => anyToType(a)
      }

  def anyToJson(value: Any): Json =
    value match {
      case v: String  => v.asJson
      case v: Int     => v.asJson
      case v: Double  => v.asJson
      case v: Instant => v.asJson
      case v: UUID    => v.asJson
      case v: Boolean => v.asJson
      case _          => throw new Exception(s"Value with unexpected type: $value")
    }

  def anyToType(value: Any): String =
    value match {
      case _: String  => "string"
      case _: Int     => "int"
      case _: Double  => "double"
      case _: Instant => "timestamp"
      case _: UUID    => "string"
      case _: Boolean => "boolean"
      case _          => throw new Exception(s"Value with unexpected type: $value")
    }
}
