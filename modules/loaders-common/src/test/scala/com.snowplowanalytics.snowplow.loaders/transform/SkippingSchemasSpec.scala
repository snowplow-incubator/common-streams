/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import cats.effect.unsafe.implicits.global
import com.snowplowanalytics.iglu.client.resolver.registries.JavaNetRegistryLookup.ioLookupInstance
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.schemaddl.parquet.Caster.NamedValue
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}
import com.snowplowanalytics.snowplow.badrows.{Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.loaders.transform.NonAtomicFieldsSpec.embeddedResolver
import io.circe.Json
import io.circe.literal._
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import java.time.Instant
import java.util.UUID

class SkippingSchemasSpec extends Specification {

  def is = s2"""
   For unstructured:             
    Skip none if there is nothing to skip ${Unstructured.e1}
    Skip none if there is no schema matching criteria ${Unstructured.e2}
    Skip all for *-*-* criteria ${Unstructured.e3}
    Skip all matching partial A-*-* criteria ${Unstructured.e4}
    Skip all matching partial A-B-* criteria ${Unstructured.e5}
    Skip all matching full A-B-C criteria ${Unstructured.e6}
    Skip all for mixed criteria: A-*-* + A-B-* + A-B-C ${Unstructured.e7}
    
   For structured: 
    Skip none if there is nothing to skip ${Structured.e1}
    Skip none if there is no schema matching criteria ${Structured.e2}
    Skip all for *-*-* criteria ${Structured.e3}
    Skip all matching partial A-*-* criteria ${Structured.e4}
    Skip all matching partial A-B-* criteria ${Structured.e5}
    Skip all matching full A-B-C criteria ${Structured.e6}
    Skip all for mixed criteria: A-*-* + A-B-* + A-B-C ${Structured.e7}
    If provided schema doesn't exist and is not skipped => failure ${Structured.e8} 
    If provided schema doesn't exist and is skipped => no failure ${Structured.e9} 
  """

  object Unstructured {
    def e1 = {
      val schemasToSkip = List.empty

      val input = inputEvent(
        withUnstruct        = Some(sdj(key = "iglu:com.example/mySchema/jsonschema/1-0-0", data = json"""{"field": "ue1"}""")),
        withContexts        = List(sdj(key = "iglu:com.example/mySchema/jsonschema/2-0-0", data = json"""{"field": "c1"}""")),
        withDerivedContexts = List(sdj(key = "iglu:com.example/mySchema/jsonschema/3-0-0", data = json"""{"field": "dc1"}"""))
      )

      assertUnstructured(input, schemasToSkip)(
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

      assertUnstructured(input, schemasToSkip)(
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

      assertUnstructured(input, schemasToSkip)(
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

      assertUnstructured(input, schemasToSkip)(
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

      assertUnstructured(input, schemasToSkip)(
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

      assertUnstructured(input, schemasToSkip)(
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

      assertUnstructured(input, schemasToSkip)(
        shouldNotExist = List(
          "unstruct_event_com_example_my_schema_1",
          "contexts_com_example_my_schema_2",
          "contexts_com_example_my_schema_3"
        ),
        shouldExist = List.empty
      )
    }
  }

  object Structured {
    def e1 = {
      val schemasToSkip = List.empty

      val input = Map(
        TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7) -> Set((0, 0)),
        TabledEntity(TabledEntity.Context, "myvendor", "myschema", 8) -> Set((0, 0)),
        TabledEntity(TabledEntity.Context, "myvendor", "myschema", 9) -> Set((0, 0))
      )

      assertStructured(input, schemasToSkip)(
        shouldNotExist = List.empty,
        shouldExist = Map(
          "unstruct_event_myvendor_myschema_7" -> Set((0, 0)),
          "contexts_myvendor_myschema_8" -> Set((0, 0)),
          "contexts_myvendor_myschema_9" -> Set((0, 0))
        ),
        failuresCount = 0
      )
    }

    def e2 = {
      val schemasToSkip = List("iglu:myvendor/myschema/jsonschema/7-8-9")

      val input = Map(
        TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7) -> Set((0, 0)),
        TabledEntity(TabledEntity.Context, "myvendor", "myschema", 8) -> Set((0, 0)),
        TabledEntity(TabledEntity.Context, "myvendor", "myschema", 9) -> Set((0, 0))
      )

      assertStructured(input, schemasToSkip)(
        shouldNotExist = List.empty,
        shouldExist = Map(
          "unstruct_event_myvendor_myschema_7" -> Set((0, 0)),
          "contexts_myvendor_myschema_8" -> Set((0, 0)),
          "contexts_myvendor_myschema_9" -> Set((0, 0))
        ),
        failuresCount = 0
      )
    }

    def e3 = {
      val schemasToSkip = List("iglu:myvendor/myschema/jsonschema/*-*-*")

      val input = Map(
        TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7) -> Set((0, 0)),
        TabledEntity(TabledEntity.Context, "myvendor", "myschema", 8) -> Set((0, 0)),
        TabledEntity(TabledEntity.Context, "myvendor", "myschema", 9) -> Set((0, 0))
      )

      assertStructured(input, schemasToSkip)(
        shouldNotExist = List(
          "unstruct_event_myvendor_myschema_7",
          "contexts_myvendor_myschema_8",
          "contexts_myvendor_myschema_9"
        ),
        shouldExist   = Map.empty,
        failuresCount = 0
      )
    }

    def e4 = {
      val schemasToSkip = List("iglu:myvendor/myschema/jsonschema/7-*-*")

      val input = Map(
        TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7) -> Set((0, 0)),
        TabledEntity(TabledEntity.Context, "myvendor", "myschema", 8) -> Set((0, 0)),
        TabledEntity(TabledEntity.Context, "myvendor", "myschema", 9) -> Set((0, 0))
      )

      assertStructured(input, schemasToSkip)(
        shouldNotExist = List(
          "unstruct_event_myvendor_myschema_7"
        ),
        shouldExist = Map(
          "contexts_myvendor_myschema_8" -> Set((0, 0)),
          "contexts_myvendor_myschema_9" -> Set((0, 0))
        ),
        failuresCount = 0
      )
    }

    def e5 = {
      val schemasToSkip = List("iglu:myvendor/myschema/jsonschema/7-0-*", "iglu:myvendor/myschema/jsonschema/8-0-*")

      val input = Map(
        TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7) -> Set((0, 0), (0, 1), (1, 0)),
        TabledEntity(TabledEntity.Context, "myvendor", "myschema", 8) -> Set((0, 0)),
        TabledEntity(TabledEntity.Context, "myvendor", "myschema", 9) -> Set((0, 0))
      )

      assertStructured(input, schemasToSkip)(
        shouldNotExist = List(
          "contexts_myvendor_myschema_8"
        ),
        shouldExist = Map(
          "unstruct_event_myvendor_myschema_7" -> Set((1, 0)),
          "contexts_myvendor_myschema_9" -> Set((0, 0))
        ),
        failuresCount = 0
      )
    }

    def e6 = {
      val schemasToSkip = List("iglu:myvendor/myschema/jsonschema/9-0-0")

      val input = Map(
        TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7) -> Set((0, 0), (1, 0)),
        TabledEntity(TabledEntity.Context, "myvendor", "myschema", 8) -> Set((0, 0)),
        TabledEntity(TabledEntity.Context, "myvendor", "myschema", 9) -> Set((0, 0))
      )

      assertStructured(input, schemasToSkip)(
        shouldNotExist = List(
          "contexts_myvendor_myschema_9"
        ),
        shouldExist = Map(
          "unstruct_event_myvendor_myschema_7" -> Set((0, 0), (0, 1), (1, 0)),
          "contexts_myvendor_myschema_8" -> Set((0, 0))
        ),
        failuresCount = 0
      )
    }

    def e7 = {
      val input = Map(
        TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7) -> Set((0, 0)),
        TabledEntity(TabledEntity.Context, "myvendor", "myschema", 8) -> Set((0, 0)),
        TabledEntity(TabledEntity.Context, "myvendor", "myschema", 9) -> Set((0, 0))
      )

      val schemasToSkip = List(
        "iglu:myvendor/myschema/jsonschema/7-*-*",
        "iglu:myvendor/myschema/jsonschema/8-0-*",
        "iglu:myvendor/myschema/jsonschema/9-0-0"
      )

      assertStructured(input, schemasToSkip)(
        shouldNotExist = List(
          "unstruct_event_myvendor_myschema_7",
          "contexts_myvendor_myschema_8",
          "contexts_myvendor_myschema_9"
        ),
        shouldExist   = Map.empty,
        failuresCount = 0
      )
    }

    def e8 = {
      val input = Map(
        // It doesn't exist in test resources and lookup by embedded resolver fails
        TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 100) -> Set((0, 0))
      )

      val schemasToSkip = List.empty

      assertStructured(input, schemasToSkip)(
        shouldNotExist = List(
          "unstruct_event_myvendor_myschema_100"
        ),
        shouldExist   = Map.empty,
        failuresCount = 1
      )
    }

    def e9 = {
      val input = Map(
        // It doesn't exist in test resources and lookup by embedded resolver fails
        TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 100) -> Set((0, 0))
      )

      val schemasToSkip = List(
        "iglu:myvendor/myschema/jsonschema/100-*-*"
      )

      assertStructured(input, schemasToSkip)(
        shouldNotExist = List(
          "unstruct_event_myvendor_myschema_100"
        ),
        shouldExist   = Map.empty,
        failuresCount = 0
      )
    }
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

  private def assertUnstructured(
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

  private def assertStructured(
    input: Map[TabledEntity, Set[SchemaSubVersion]],
    schemasToSkip: List[String]
  )(
    shouldNotExist: List[String],
    shouldExist: Map[String, Set[SchemaSubVersion]],
    failuresCount: Int
  ): MatchResult[Any] = {
    val criterion = schemasToSkip.map(schemas => SchemaCriterion.parse(schemas).get)
    NonAtomicFields
      .resolveTypes(embeddedResolver, input, criterion)
      .map {
        case Right(output) =>
          val mapped = output.fields.map(entity => (entity.mergedField.name, entity.mergedVersions)).toMap

          val assertNotExist: MatchResult[Any] = output.fields.map(_.mergedField.name) must not(containAnyOf(shouldNotExist))
          val assertExists: MatchResult[Any]   = mapped must beEqualTo(shouldExist)
          val assertFailures: MatchResult[Any] = output.igluFailures.size must beEqualTo(failuresCount)
          assertNotExist and assertExists and assertFailures
        case Left(_) => ko
      }
      .unsafeRunSync()
  }
}
