/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.schemaddl.parquet.Caster.NamedValue
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}
import com.snowplowanalytics.snowplow.badrows.{Processor => BadRowProcessor}
import io.circe.Json
import io.circe.literal._
import org.specs2.matcher.MatchResult
import org.specs2.Specification

import java.time.Instant
import java.util.UUID

class SkippingSchemasSpec extends Specification {

  def is = s2"""
  Skip none if there is nothing to skip $e1
  Skip none if there is no schema matching criteria $e2
  Skip all for *-*-* criteria $e3
  Skip all matching partial A-*-* criteria $e4
  Skip all matching partial A-B-* criteria $e5
  Skip all matching full A-B-C criteria $e6
  Skip all for mixed criteria: A-*-* + A-B-* + A-B-C $e7
  """

  def e1 = {
    val schemasToSkip = List.empty

    val input = inputEvent(
      withUnstruct        = Some(sdj(key = "iglu:com.example/mySchema/jsonschema/1-0-0", data = json"""{"field": "ue1"}""")),
      withContexts        = List(sdj(key = "iglu:com.example/mySchema/jsonschema/2-0-0", data = json"""{"field": "c1"}""")),
      withDerivedContexts = List(sdj(key = "iglu:com.example/mySchema/jsonschema/3-0-0", data = json"""{"field": "dc1"}"""))
    )

    assert(input, schemasToSkip)(
      shouldNotExist = List.empty,
      shouldExist = List(
        NamedValue(name = "unstruct_event_com_example_my_schema_1", value = json"""{"field": "ue1"}"""),
        NamedValue(
          name  = "contexts_com_example_my_schema_2",
          value = json"""[{"_schema_version" : "2-0-0", "field": "c1"}]"""
        ),
        NamedValue(
          name  = "contexts_com_example_my_schema_3",
          value = json"""[{"_schema_version" : "3-0-0", "field": "dc1"}]"""
        )
      )
    )
  }

  def e2 = {
    val schemasToSkip = List("iglu:com.example/mySchema/jsonschema/1-2-3")

    val input = inputEvent(
      withUnstruct        = Some(sdj(key = "iglu:com.example/mySchema/jsonschema/1-0-0", data = json"""{"field": "ue1"}""")),
      withContexts        = List(sdj(key = "iglu:com.example/mySchema/jsonschema/2-0-0", data = json"""{"field": "c1"}""")),
      withDerivedContexts = List(sdj(key = "iglu:com.example/mySchema/jsonschema/3-0-0", data = json"""{"field": "dc1"}"""))
    )

    assert(input, schemasToSkip)(
      shouldNotExist = List.empty,
      shouldExist = List(
        NamedValue(name = "unstruct_event_com_example_my_schema_1", value = json"""{"field": "ue1"}"""),
        NamedValue(
          name  = "contexts_com_example_my_schema_2",
          value = json"""[{"_schema_version" : "2-0-0", "field": "c1"}]"""
        ),
        NamedValue(
          name  = "contexts_com_example_my_schema_3",
          value = json"""[{"_schema_version" : "3-0-0", "field": "dc1"}]"""
        )
      )
    )
  }

  def e3 = {
    val schemasToSkip = List("iglu:com.example/mySchema/jsonschema/*-*-*")

    val input = inputEvent(
      withUnstruct        = Some(sdj(key = "iglu:com.example/mySchema/jsonschema/1-0-0", data = json"""{"field": "ue1"}""")),
      withContexts        = List(sdj(key = "iglu:com.example/mySchema/jsonschema/2-0-0", data = json"""{"field": "c1"}""")),
      withDerivedContexts = List(sdj(key = "iglu:com.example/mySchema/jsonschema/3-0-0", data = json"""{"field": "dc1"}"""))
    )

    assert(input, schemasToSkip)(
      shouldNotExist = List(
        "unstruct_event_com_example_my_schema_1",
        "contexts_com_example_my_schema_2",
        "contexts_com_example_my_schema_3"
      ),
      shouldExist = List.empty
    )
  }

  def e4 = {
    val schemasToSkip = List("iglu:com.example/mySchema/jsonschema/1-*-*")

    val input = inputEvent(
      withUnstruct        = Some(sdj(key = "iglu:com.example/mySchema/jsonschema/1-0-0", data = json"""{"field": "ue1"}""")),
      withContexts        = List(sdj(key = "iglu:com.example/mySchema/jsonschema/2-0-0", data = json"""{"field": "c1"}""")),
      withDerivedContexts = List(sdj(key = "iglu:com.example/mySchema/jsonschema/3-0-0", data = json"""{"field": "dc1"}"""))
    )

    assert(input, schemasToSkip)(
      shouldNotExist = List(
        "unstruct_event_com_example_my_schema_1"
      ),
      shouldExist = List(
        NamedValue(
          name  = "contexts_com_example_my_schema_2",
          value = json"""[{"_schema_version" : "2-0-0", "field": "c1"}]"""
        ),
        NamedValue(
          name  = "contexts_com_example_my_schema_3",
          value = json"""[{"_schema_version" : "3-0-0", "field": "dc1"}]"""
        )
      )
    )
  }

  def e5 = {
    val schemasToSkip = List("iglu:com.example/mySchema/jsonschema/1-0-*", "iglu:com.example/mySchema/jsonschema/2-0-*")

    val input = inputEvent(
      withUnstruct = Some(sdj(key = "iglu:com.example/mySchema/jsonschema/1-0-0", data = json"""{"field": "ue1"}""")),
      withContexts = List(
        sdj(key = "iglu:com.example/mySchema/jsonschema/2-0-0", data = json"""{"field": "c1"}"""),
        sdj(key = "iglu:com.example/mySchema/jsonschema/2-1-0", data = json"""{"field": "c2"}""")
      ),
      withDerivedContexts = List(sdj(key = "iglu:com.example/mySchema/jsonschema/3-0-0", data = json"""{"field": "dc1"}"""))
    )

    assert(input, schemasToSkip)(
      shouldNotExist = List(
        "unstruct_event_com_example_my_schema_1"
      ),
      shouldExist = List(
        // There is still '_2' column because 2-0-0 is skipped, but 2-1-0 with c2 value is not
        NamedValue(
          name  = "contexts_com_example_my_schema_2",
          value = json"""[{"_schema_version" : "2-1-0", "field": "c2"}]"""
        ),
        NamedValue(
          name  = "contexts_com_example_my_schema_3",
          value = json"""[{"_schema_version" : "3-0-0", "field": "dc1"}]"""
        )
      )
    )
  }

  def e6 = {
    val schemasToSkip = List("iglu:com.example/mySchema/jsonschema/3-0-0")

    val input = inputEvent(
      withUnstruct = Some(sdj(key = "iglu:com.example/mySchema/jsonschema/1-0-0", data = json"""{"field": "ue1"}""")),
      withContexts = List(
        sdj(key = "iglu:com.example/mySchema/jsonschema/2-0-0", data = json"""{"field": "c1"}"""),
        sdj(key = "iglu:com.example/mySchema/jsonschema/2-1-0", data = json"""{"field": "c2"}""")
      ),
      withDerivedContexts = List(sdj(key = "iglu:com.example/mySchema/jsonschema/3-0-0", data = json"""{"field": "dc1"}"""))
    )

    assert(input, schemasToSkip)(
      shouldNotExist = List(
        "contexts_com_example_my_schema_3"
      ),
      shouldExist = List(
        NamedValue(name = "unstruct_event_com_example_my_schema_1", value = json"""{"field": "ue1"}"""),
        NamedValue(
          name = "contexts_com_example_my_schema_2",
          value = json"""[{"_schema_version" : "2-0-0", "field": "c1"}, 
                          {"_schema_version" : "2-1-0", "field": "c2"}]"""
        )
      )
    )
  }

  def e7 = {
    val input = inputEvent(
      withUnstruct        = Some(sdj(key = "iglu:com.example/mySchema/jsonschema/1-0-0", data = json"""{"field": "ue1"}""")),
      withContexts        = List(sdj(key = "iglu:com.example/mySchema/jsonschema/2-0-0", data = json"""{"field": "c1"}""")),
      withDerivedContexts = List(sdj(key = "iglu:com.example/mySchema/jsonschema/3-0-0", data = json"""{"field": "dc1"}"""))
    )

    val schemasToSkip = List(
      "iglu:com.example/mySchema/jsonschema/1-*-*",
      "iglu:com.example/mySchema/jsonschema/2-0-*",
      "iglu:com.example/mySchema/jsonschema/3-0-0"
    )

    assert(input, schemasToSkip)(
      shouldNotExist = List(
        "unstruct_event_com_example_my_schema_1",
        "contexts_com_example_my_schema_2",
        "contexts_com_example_my_schema_3"
      ),
      shouldExist = List.empty
    )
  }

  private def inputEvent(
    withUnstruct: Option[SelfDescribingData[Json]],
    withContexts: List[SelfDescribingData[Json]],
    withDerivedContexts: List[SelfDescribingData[Json]]
  ): Event =
    Event
      .minimal(UUID.randomUUID, Instant.now, "0.0.0", "0.0.0")
      .copy(unstruct_event = UnstructEvent(withUnstruct))
      .copy(contexts = Contexts(withContexts))
      .copy(derived_contexts = Contexts(withDerivedContexts))

  private def sdj(key: String, data: Json): SelfDescribingData[Json] =
    SelfDescribingData[Json](SchemaKey.fromUri(key).toOption.get, data)

  private def assert(
    input: Event,
    schemasToSkip: List[String]
  )(
    shouldNotExist: List[String],
    shouldExist: List[NamedValue[Json]]
  ): MatchResult[Any] = {
    val badProcessor = BadRowProcessor("snowflake-loader", "0.0.0")

    val output = Transform
      .transformEventUnstructured(
        badProcessor,
        TestCaster,
        TestCirceFolder,
        input,
        schemasToSkip.map(schemas => SchemaCriterion.parse(schemas).get)
      )
      .toOption
      .get

    output.map(_.name) must not(containAnyOf(shouldNotExist)) and
      (output must containAllOf(shouldExist))
  }
}
