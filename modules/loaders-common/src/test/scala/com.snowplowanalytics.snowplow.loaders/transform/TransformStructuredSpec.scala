/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import cats.data.NonEmptyList
import com.snowplowanalytics.iglu.client.ClientError.ResolutionError
import com.snowplowanalytics.iglu.core.SchemaVer.Full
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.schemaddl.parquet.Caster.NamedValue
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.Nullability.{Nullable, Required}
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Event, SnowplowEvent}
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.loaders.transform.NonAtomicFields.{ColumnFailure, Result}
import io.circe._
import io.circe.literal._
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import java.time.Instant
import java.util.UUID
import scala.collection.immutable.SortedMap

class TransformStructuredSpec extends Specification {

  private val simpleOneFieldSchema =
    List(
      Field("my_string", Type.String, Required)
    )

  private val schemaWithAllPossibleTypes =
    List(
      Field("my_string", Type.String, Required),
      Field("my_int", Type.Integer, Required),
      Field("my_long", Type.Long, Required),
      Field("my_decimal", Type.Decimal(Type.DecimalPrecision.Digits9, 2), Required),
      Field("my_double", Type.Double, Required),
      Field("my_boolean", Type.Boolean, Required),
      Field("my_date", Type.Date, Required),
      Field("my_timestamp", Type.Timestamp, Required),
      Field("my_array", Type.Array(Type.Integer, Required), Required),
      Field("my_object", Type.Struct(List(Field("abc", Type.String, Required))), Required),
      Field("my_null", Type.String, Nullable)
    )

  private val dataWithAllTypes = json"""
   {
     "my_string":   "abc",
     "my_int":       42,
     "my_long":      42000000000,
     "my_decimal":   1.23,
     "my_double":    1.2323,
     "my_boolean":   true,
     "my_date":      "2024-03-19",
     "my_timestamp": "2024-03-19T20:20:39Z",
     "my_array":     [1,2,3],
     "my_object":    {"abc": "xyz"},
     "my_null":      null
   }
   """

  private val now     = Instant.now
  private val eventId = UUID.randomUUID()

  def is = s2"""
    Successful transformation:
      Valid event with only atomic fields (no custom entities) $onlyAtomic
      Valid event with one custom context $oneContext
      Valid unstruct event $unstruct
      Valid event with two custom contexts, same major version $twoContextsSameMajor
      Valid event with two custom contexts, different major version $twoContextsDifferentMajor
      Valid event with each different type of atomic field $onlyAtomicAllTypes
      Valid unstruct event with each different type $unstructAllTypes
      Valid event with one context and each different type $contextAllTypes
      No unstruct column when there is unstruct data but no related to schema family type in a batch $unstructNoFamily
      No contexts column when there is context data but no related to schema family type in a batch $contextNoFamily
      JSON null on output for unstruct column if no data matching type is provided $unstructNoData
      JSON null on output for contexts column if no data matching type is provided $contextsNoData
      JSON null on output for unstruct column if no sub-version matching data exists in a batch $unstructNoMatchingSubVersion
      JSON null on output for contexts column if no sub-version matching data exists in a batch $contextNoMatchingSubVersion
   
    Failures:
      Atomic currency field cannot be cast to a decimal due to rounding $atomicTooManyDecimalPoints
      Atomic currency field cannot be cast to a decimal due to high precision $atomicHighPrecision
      Missing value for unstruct (null passed in required field) $unstructMissingValue
      Missing value for context (null passed in required field) $contextMissingValue
      Cast error for unstruct (integer passed in string field) $unstructWrongType
      Cast error for context (integer passed in string field) $contextWrongType
      Iglu error in batch info becomes iglu transformation error $igluErrorInBatchInfo
  """

  def onlyAtomic = {
    val event     = createEvent()
    val batchInfo = Result(List.empty, List.empty) // no custom entities
    val expectedOutput = List(
      NamedValue("event_id", Json.fromString(eventId.toString)),
      NamedValue("collector_tstamp", Json.fromString(now.toString)),
      NamedValue("geo_region", Json.Null)
    )

    assertSuccessful(event, batchInfo, expectedOutput)
  }

  def unstruct = {
    val inputEvent =
      createEvent(unstruct = Some(sdj(data = json"""{ "my_string": "abc"}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = Result(
      fields       = List(mySchemaUnstruct(model = 1, subVersions = Set((0, 0)))),
      igluFailures = List.empty
    )
    val expectedOutput = List(NamedValue(name = "unstruct_event_com_example_my_schema_1", value = json"""{ "my_string": "abc"}"""))

    assertSuccessful(inputEvent, batchInfo, expectedOutput)
  }

  def oneContext = {
    val inputEvent =
      createEvent(contexts = List(sdj(data = json"""{ "my_string": "abc"}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = Result(
      fields       = List(mySchemaContexts(model = 1, subVersions = Set((0, 0)))),
      igluFailures = List.empty
    )
    val expectedOutput = List(
      NamedValue(name = "contexts_com_example_my_schema_1", value = json"""[{ "_schema_version": "1-0-0", "my_string": "abc"}]""")
    )

    assertSuccessful(inputEvent, batchInfo, expectedOutput)
  }

  def twoContextsDifferentMajor = {
    val inputEvent = createEvent(contexts =
      List(
        sdj(data = json"""{ "my_string": "abc"}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0"),
        sdj(data = json"""{ "my_string": "abc"}""", key = "iglu:com.example/mySchema/jsonschema/2-0-0")
      )
    )

    val batchInfo = Result(
      fields = List(
        mySchemaContexts(model = 1, subVersions = Set((0, 0))),
        mySchemaContexts(model = 2, subVersions = Set((0, 0)))
      ),
      igluFailures = List.empty
    )
    val expectedOutput = List(
      NamedValue(
        name  = "contexts_com_example_my_schema_1",
        value = Json.arr(json"""{ "_schema_version": "1-0-0", "my_string": "abc"}""")
      ),
      NamedValue(
        name  = "contexts_com_example_my_schema_2",
        value = Json.arr(json"""{ "_schema_version": "2-0-0", "my_string": "abc"}""")
      )
    )

    assertSuccessful(inputEvent, batchInfo, expectedOutput)
  }

  def twoContextsSameMajor = {
    val inputEvent = createEvent(contexts =
      List(
        sdj(data = json"""{ "my_string": "abc"}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0"),
        sdj(data = json"""{ "my_string": "abc"}""", key = "iglu:com.example/mySchema/jsonschema/1-1-0")
      )
    )

    val batchTypesInfo = Result(
      fields = List(
        mySchemaContexts(model = 1, subVersions = Set((0, 0), (1, 0)))
      ),
      igluFailures = List.empty
    )
    val expectedOutput = List(
      NamedValue(
        name = "contexts_com_example_my_schema_1",
        value = Json.arr(
          json"""{ "_schema_version": "1-0-0", "my_string": "abc"}""",
          json"""{ "_schema_version": "1-1-0", "my_string": "abc"}"""
        )
      )
    )

    assertSuccessful(inputEvent, batchTypesInfo, expectedOutput)
  }

  def onlyAtomicAllTypes = {
    val event = createEvent()
      .copy(
        app_id              = Some("myapp"),
        dvce_created_tstamp = Some(now),
        txn_id              = Some(42),
        geo_latitude        = Some(1.234),
        dvce_ismobile       = Some(true),
        tr_total            = Some(12.34)
      )

    val batchInfo = Result(List.empty, List.empty) // no custom entities
    val expectedOutput = List(
      NamedValue("app_id", json""" "myapp" """),
      NamedValue("dvce_created_tstamp", json"$now"),
      NamedValue("txn_id", json"42"),
      NamedValue("geo_latitude", json"1.234"),
      NamedValue("dvce_ismobile", json"true"),
      NamedValue("tr_total", json"12.34")
    )

    assertSuccessful(event, batchInfo, expectedOutput)
  }

  def unstructAllTypes = {
    val inputEvent =
      createEvent(unstruct = Some(sdj(data = dataWithAllTypes, key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = Result(
      fields       = List(mySchemaUnstruct(model = 1, subVersions = Set((0, 0)), schemaWithAllPossibleTypes)),
      igluFailures = List.empty
    )
    val expectedOutput = List(
      NamedValue(
        name  = "unstruct_event_com_example_my_schema_1",
        value = dataWithAllTypes
      )
    )

    assertSuccessful(inputEvent, batchInfo, expectedOutput)
  }

  def contextAllTypes = {
    val inputEvent =
      createEvent(contexts = List(sdj(data = dataWithAllTypes, key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = Result(
      fields       = List(mySchemaContexts(model = 1, subVersions = Set((0, 0)), schemaWithAllPossibleTypes)),
      igluFailures = List.empty
    )
    val expectedOutput = List(
      NamedValue(
        name = "contexts_com_example_my_schema_1",
        value = json"""
              [{
                "_schema_version": "1-0-0",
                "my_string":       "abc",
                "my_int":           42,
                "my_long":          42000000000,
                "my_decimal":       1.23,
                "my_double":        1.2323,
                "my_boolean":       true,
                "my_date":          "2024-03-19",
                "my_timestamp":     "2024-03-19T20:20:39Z",
                "my_array":         [1,2,3],
                "my_object":        {"abc": "xyz"},
                "my_null":          null
              }]
              """
      )
    )

    assertSuccessful(inputEvent, batchInfo, expectedOutput)
  }

  def unstructNoData = {
    val inputEvent = createEvent()

    val batchTypesInfo = Result(
      fields = List(
        mySchemaUnstruct(model = 1, subVersions = Set((0, 0)))
      ),
      igluFailures = List.empty
    )
    val expectedOutput = List(
      NamedValue(
        name  = "unstruct_event_com_example_my_schema_1",
        value = Json.Null
      )
    )

    assertSuccessful(inputEvent, batchTypesInfo, expectedOutput)
  }

  def contextsNoData = {
    val inputEvent = createEvent()

    val batchTypesInfo = Result(
      fields = List(
        mySchemaContexts(model = 1, subVersions = Set((0, 0)))
      ),
      igluFailures = List.empty
    )
    val expectedOutput = List(
      NamedValue(
        name  = "contexts_com_example_my_schema_1",
        value = Json.Null
      )
    )

    assertSuccessful(inputEvent, batchTypesInfo, expectedOutput)
  }

  def unstructNoFamily = {
    val inputEvent =
      createEvent(unstruct = Some(sdj(data = json"""{ "my_string": "abc"}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))

    val batchTypesInfo = Result(
      fields       = List.empty,
      igluFailures = List.empty
    )
    val shouldNotExist = List("unstruct_event_com_example_my_schema_1")

    assertSuccessful(inputEvent, batchTypesInfo, shouldNotExist = shouldNotExist)
  }

  def unstructNoMatchingSubVersion = {
    val inputEvent =
      createEvent(unstruct = Some(sdj(data = json"""{ "my_string": "abc"}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))

    val batchTypesInfo = Result(
      fields       = List(mySchemaUnstruct(model = 1, subVersions = Set((1, 0)))),
      igluFailures = List.empty
    )

    val expectedOutput = List(
      NamedValue(
        name  = "unstruct_event_com_example_my_schema_1",
        value = Json.Null // data is 1-0-0, subVersion type is 1-1-0. Column exists but with null value.
      )
    )

    assertSuccessful(inputEvent, batchTypesInfo, expectedOutput)
  }

  def contextNoFamily = {
    val inputEvent = createEvent(contexts =
      List(
        sdj(data = json"""{ "my_string": "abc"}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")
      )
    )

    val batchTypesInfo = Result(
      fields       = List.empty,
      igluFailures = List.empty
    )
    val shouldNotExist = List("contexts_com_example_my_schema_1")

    assertSuccessful(inputEvent, batchTypesInfo, shouldNotExist = shouldNotExist)
  }

  def contextNoMatchingSubVersion = {
    val inputEvent = createEvent(contexts =
      List(
        sdj(data = json"""{ "my_string": "abc"}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")
      )
    )

    val batchTypesInfo = Result(
      fields       = List(mySchemaContexts(model = 1, subVersions = Set((1, 0)))),
      igluFailures = List.empty
    )

    val expectedOutput = List(
      NamedValue(
        name  = "contexts_com_example_my_schema_1",
        value = Json.Null
      )
    )

    assertSuccessful(inputEvent, batchTypesInfo, expectedOutput)
  }

  def atomicTooManyDecimalPoints = {
    val inputEvent = createEvent()
      .copy(
        tr_total = Some(12.3456) // Too many decimal points
      )

    val batchInfo = Result(List.empty, List.empty)

    val expectedError = FailureDetails.LoaderIgluError.WrongType(
      SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", Full(1, 0, 0)),
      value    = json"12.3456",
      expected = "Decimal(Digits18,2)"
    )

    assertLoaderError(inputEvent, batchInfo, expectedError)
  }

  def atomicHighPrecision = {
    val inputEvent = createEvent()
      .copy(
        tr_total = Some(12345678987654321.34) // Too high precision
      )

    val batchInfo = Result(List.empty, List.empty)

    val expectedError = FailureDetails.LoaderIgluError.WrongType(
      SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", Full(1, 0, 0)),
      value    = json"1.2345678987654322E16",
      expected = "Decimal(Digits18,2)"
    )

    assertLoaderError(inputEvent, batchInfo, expectedError)
  }

  def unstructMissingValue = {
    val inputEvent =
      createEvent(unstruct = Some(sdj(data = json"""{ "my_string": null}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = Result(
      fields       = List(mySchemaUnstruct(model = 1, subVersions = Set((0, 0)))),
      igluFailures = List.empty
    )

    val expectedError = FailureDetails.LoaderIgluError.MissingInValue(
      SchemaKey("com.example", "mySchema", "jsonschema", Full(1, 0, 0)),
      key   = "my_string",
      value = json"""{ "my_string": null}"""
    )

    assertLoaderError(inputEvent, batchInfo, expectedError)
  }

  def unstructWrongType = {
    val inputEvent =
      createEvent(unstruct = Some(sdj(data = json"""{ "my_string": 123}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = Result(
      fields       = List(mySchemaUnstruct(model = 1, subVersions = Set((0, 0)))),
      igluFailures = List.empty
    )

    val expectedError = FailureDetails.LoaderIgluError.WrongType(
      SchemaKey("com.example", "mySchema", "jsonschema", Full(1, 0, 0)),
      value    = json"123",
      expected = "String"
    )

    assertLoaderError(inputEvent, batchInfo, expectedError)
  }

  def contextMissingValue = {
    val inputEvent =
      createEvent(contexts = List(sdj(data = json"""{ "my_string": null}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = Result(
      fields       = List(mySchemaContexts(model = 1, subVersions = Set((0, 0)))),
      igluFailures = List.empty
    )

    val expectedError = FailureDetails.LoaderIgluError.MissingInValue(
      SchemaKey("com.example", "mySchema", "jsonschema", Full(1, 0, 0)),
      key   = "my_string",
      value = json"""{  "_schema_version": "1-0-0", "my_string": null}"""
    )

    assertLoaderError(inputEvent, batchInfo, expectedError)
  }

  def contextWrongType = {
    val inputEvent =
      createEvent(contexts = List(sdj(data = json"""{ "my_string": 123}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))
    val batchInfo = Result(
      fields       = List(mySchemaContexts(model = 1, subVersions = Set((0, 0)))),
      igluFailures = List.empty
    )

    val expectedError = FailureDetails.LoaderIgluError.WrongType(
      SchemaKey("com.example", "mySchema", "jsonschema", Full(1, 0, 0)),
      value    = json"123",
      expected = "String"
    )

    assertLoaderError(inputEvent, batchInfo, expectedError)
  }

  def igluErrorInBatchInfo = {
    val inputEvent =
      createEvent(unstruct = Some(sdj(data = json"""{ "my_string": "abc"}""", key = "iglu:com.example/mySchema/jsonschema/1-0-0")))

    val igluResolutionError = FailureDetails.LoaderIgluError.SchemaListNotFound(
      SchemaCriterion("com.example", "mySchema", "jsonschema", 1),
      ResolutionError(SortedMap.empty)
    )

    val batchInfo = Result(
      fields = List.empty,
      igluFailures = List(
        ColumnFailure(
          TabledEntity(TabledEntity.UnstructEvent, "com.example", "mySchema", 1),
          Set((0, 0)),
          igluResolutionError
        )
      )
    )

    assertLoaderError(inputEvent, batchInfo, expectedError = igluResolutionError)
  }

  private def assertSuccessful(
    event: Event,
    batchInfo: Result,
    shouldExist: List[NamedValue[Json]] = List.empty,
    shouldNotExist: List[String]        = List.empty
  ) = {
    val result = Transform.transformEvent[Json](BadRowProcessor("test-loader", "0.0.0"), TestCaster, event, batchInfo)

    result must beRight { actualValues: Vector[NamedValue[Json]] =>
      val actualFieldNames = actualValues.map(_.name)

      val assertExist: MatchResult[Any]    = actualValues must containAllOf(shouldExist)
      val assertNotExist: MatchResult[Any] = actualFieldNames must not(containAnyOf(shouldNotExist))
      val noDuplicates: MatchResult[Any]   = actualFieldNames.distinct.size must beEqualTo(actualFieldNames.size)
      assertExist and assertNotExist and noDuplicates
    }
  }

  private def assertLoaderError(
    inputEvent: Event,
    batchInfo: Result,
    expectedError: FailureDetails.LoaderIgluError
  ): MatchResult[Either[BadRow, Vector[NamedValue[Json]]]] = {
    val result = Transform.transformEvent(BadRowProcessor("loader", "0.0.0"), TestCaster, inputEvent, batchInfo)

    result must beLeft.like { case BadRow.LoaderIgluError(_, Failure.LoaderIgluErrors(NonEmptyList(error, Nil)), _) =>
      error must beEqualTo(expectedError)
    }
  }

  private def createEvent(unstruct: Option[SelfDescribingData[Json]] = None, contexts: List[SelfDescribingData[Json]] = List.empty): Event =
    Event
      .minimal(eventId, now, "0.0.0", "0.0.0")
      .copy(unstruct_event = SnowplowEvent.UnstructEvent(unstruct))
      .copy(contexts = SnowplowEvent.Contexts(contexts))

  private def sdj(data: Json, key: String): SelfDescribingData[Json] =
    SelfDescribingData[Json](SchemaKey.fromUri(key).toOption.get, data)

  private def mySchemaUnstruct(
    model: Int,
    subVersions: Set[SchemaSubVersion],
    ddl: List[Field] = simpleOneFieldSchema
  ) = TypedTabledEntity(
    tabledEntity   = TabledEntity(TabledEntity.UnstructEvent, "com.example", "mySchema", model),
    mergedField    = Field(s"unstruct_event_com_example_my_schema_$model", Type.Struct(ddl), Nullable, Set.empty),
    mergedVersions = subVersions,
    recoveries     = Nil
  )

  private def mySchemaContexts(
    model: Int,
    subVersions: Set[SchemaSubVersion],
    ddl: List[Field] = simpleOneFieldSchema
  ): TypedTabledEntity = {
    val withSchemaVersion = Field("_schema_version", Type.String, Required) :: ddl
    TypedTabledEntity(
      tabledEntity = TabledEntity(TabledEntity.Context, "com.example", "mySchema", model),
      mergedField =
        Field(s"contexts_com_example_my_schema_$model", Type.Array(Type.Struct(withSchemaVersion), Required), Nullable, Set.empty),
      mergedVersions = subVersions,
      recoveries     = Nil
    )
  }
}
