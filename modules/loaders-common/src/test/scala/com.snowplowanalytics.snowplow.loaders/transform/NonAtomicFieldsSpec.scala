/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import cats.effect.IO
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.JavaNetRegistryLookup._
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.Nullability.{Nullable, Required}
import com.snowplowanalytics.snowplow.badrows.FailureDetails

class NonAtomicFieldsSpec extends Specification with CatsEffect {
  import NonAtomicFieldsSpec._

  def is = s2"""
  NonAtomicFields
    when resolving for known schemas in unstruct_event should
      return an un-merged schema if the batch uses the first schema in a series $ue1
      return a merged schema if the batch uses the last schema in a series $ue2
      return a merged schema if the batch uses all schemas in a series $ue3

    when resolving for known schemas in contexts should
      return an un-merged schema if the batch uses the first schema in a series $c1
      return a merged schema if the batch uses the last schema in a series $c2
      return a merged schema if the batch uses all schemas in a series $c3

    when resolving for known schema in contexts and unstruct_event should
      return separate entity for the context and the unstruct_event $both1

    when resolving for schemas in a series that violates schema evolution rules should
      return no recovery schemas if the batch uses the first schema in the series $recovery1
      return recovery schemas if the batch uses schemas that broke the rules $recovery2
      return recovery schemas and merged base schema if the batch uses schemas that broke the rules and those that obeyed rules $recovery3

    when handling Iglu failures should
      return a IgluError if schema key does not exist in a valid series of schemas $fail1
      return a SchemaListNotFound if the series of schemas does not exist $fail2
      return an InvalidSchema if the series contains a schema that cannot be parsed $fail3
  """

  def ue1 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        List(
          Field("col_a", Type.String, Required)
        )
      )

      val expectedField = Field("unstruct_event_myvendor_myschema_7", expectedStruct, Nullable, Set.empty)

      TypedTabledEntity(
        tabledEntity,
        expectedField,
        Set((0, 0)),
        Nil
      )
    }

    NonAtomicFields.resolveTypes(embeddedResolver, input).map { case NonAtomicFields.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields.head must beEqualTo(expected))
    }

  }

  def ue2 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((1, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        List(
          Field("col_a", Type.String, Required),
          Field("col_c", Type.String, Nullable),
          Field("col_b", Type.String, Nullable)
        )
      )

      val expectedField = Field("unstruct_event_myvendor_myschema_7", expectedStruct, Nullable, Set.empty)

      TypedTabledEntity(
        tabledEntity,
        expectedField,
        Set((0, 0), (0, 1), (1, 0)),
        Nil
      )
    }

    NonAtomicFields.resolveTypes(embeddedResolver, input).map { case NonAtomicFields.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields.head must beEqualTo(expected))
    }

  }

  def ue3 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 0), (0, 1), (1, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        List(
          Field("col_a", Type.String, Required),
          Field("col_c", Type.String, Nullable),
          Field("col_b", Type.String, Nullable)
        )
      )

      val expectedField = Field("unstruct_event_myvendor_myschema_7", expectedStruct, Nullable, Set.empty)

      TypedTabledEntity(
        tabledEntity,
        expectedField,
        Set((0, 0), (0, 1), (1, 0)),
        Nil
      )
    }

    NonAtomicFields.resolveTypes(embeddedResolver, input).map { case NonAtomicFields.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields.head must beEqualTo(expected))
    }

  }

  def c1 = {

    val tabledEntity = TabledEntity(TabledEntity.Context, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        List(
          Field("_schema_version", Type.String, Required),
          Field("col_a", Type.String, Required)
        )
      )

      val expectedArray = Type.Array(expectedStruct, Required)

      val expectedField = Field("contexts_myvendor_myschema_7", expectedArray, Nullable, Set.empty)

      TypedTabledEntity(
        tabledEntity,
        expectedField,
        Set((0, 0)),
        Nil
      )
    }

    NonAtomicFields.resolveTypes(embeddedResolver, input).map { case NonAtomicFields.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields.head must beEqualTo(expected))
    }

  }

  def c2 = {

    val tabledEntity = TabledEntity(TabledEntity.Context, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((1, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        List(
          Field("_schema_version", Type.String, Required),
          Field("col_a", Type.String, Required),
          Field("col_c", Type.String, Nullable),
          Field("col_b", Type.String, Nullable)
        )
      )

      val expectedArray = Type.Array(expectedStruct, Required)

      val expectedField = Field("contexts_myvendor_myschema_7", expectedArray, Nullable, Set.empty)

      TypedTabledEntity(
        tabledEntity,
        expectedField,
        Set((0, 0), (0, 1), (1, 0)),
        Nil
      )
    }

    NonAtomicFields.resolveTypes(embeddedResolver, input).map { case NonAtomicFields.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields.head must beEqualTo(expected))
    }

  }

  def c3 = {

    val tabledEntity = TabledEntity(TabledEntity.Context, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 0), (0, 1), (1, 0))
    )

    val expected = {

      val expectedStruct = Type.Struct(
        List(
          Field("_schema_version", Type.String, Required),
          Field("col_a", Type.String, Required),
          Field("col_c", Type.String, Nullable),
          Field("col_b", Type.String, Nullable)
        )
      )

      val expectedArray = Type.Array(expectedStruct, Required)

      val expectedField = Field("contexts_myvendor_myschema_7", expectedArray, Nullable, Set.empty)

      TypedTabledEntity(
        tabledEntity,
        expectedField,
        Set((0, 0), (0, 1), (1, 0)),
        Nil
      )
    }

    NonAtomicFields.resolveTypes(embeddedResolver, input).map { case NonAtomicFields.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields.head must beEqualTo(expected))
    }

  }

  def both1 = {

    val tabledEntity1 = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)
    val tabledEntity2 = TabledEntity(TabledEntity.Context, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity1 -> Set((0, 0)),
      tabledEntity2 -> Set((0, 0))
    )

    NonAtomicFields.resolveTypes(embeddedResolver, input).map { case NonAtomicFields.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(2)) and
        (fields.map(_.tabledEntity) must contain(allOf(tabledEntity1, tabledEntity2)))
    }

  }

  def recovery1 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "bad_schema_evolution", 1)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        List(
          Field("col_a", Type.String, Required)
        )
      )

      val expectedField = Field("unstruct_event_myvendor_bad_schema_evolution_1", expectedStruct, Nullable, Set.empty)

      TypedTabledEntity(
        tabledEntity,
        expectedField,
        Set((0, 0)),
        Nil
      )
    }

    NonAtomicFields.resolveTypes(embeddedResolver, input).map { case NonAtomicFields.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields.head must beEqualTo(expected))
    }

  }

  def recovery2 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "bad_schema_evolution", 1)

    val input = Map(
      tabledEntity -> Set((0, 0), (0, 1), (0, 2))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        List(
          Field("col_a", Type.String, Required)
        )
      )

      val recoveryStruct1 = Type.Struct(
        List(
          Field("col_a", Type.Double, Required)
        )
      )

      val recoveryStruct2 = Type.Struct(
        List(
          Field("col_a", Type.Boolean, Required)
        )
      )

      val expectedField = Field("unstruct_event_myvendor_bad_schema_evolution_1", expectedStruct, Nullable, Set.empty)

      val recoveryField1 =
        Field("unstruct_event_myvendor_bad_schema_evolution_1_recovered_1_0_1_e7cf2565", recoveryStruct1, Nullable, Set.empty)
      val recoveryField2 =
        Field("unstruct_event_myvendor_bad_schema_evolution_1_recovered_1_0_2_cacf6738", recoveryStruct2, Nullable, Set.empty)

      TypedTabledEntity(
        tabledEntity,
        expectedField,
        Set((0, 0)),
        List(((0, 1), recoveryField1), ((0, 2), recoveryField2))
      )
    }

    NonAtomicFields.resolveTypes(embeddedResolver, input).map { case NonAtomicFields.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields.head must beEqualTo(expected))
    }

  }

  def recovery3 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "bad_schema_evolution", 1)

    val input = Map(
      tabledEntity -> Set((0, 0), (0, 1), (0, 2), (0, 3))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        List(
          Field("col_a", Type.String, Required),
          Field("col_b", Type.Long, Nullable)
        )
      )

      val recoveryStruct1 = Type.Struct(
        List(
          Field("col_a", Type.Double, Required)
        )
      )

      val recoveryStruct2 = Type.Struct(
        List(
          Field("col_a", Type.Boolean, Required)
        )
      )

      val expectedField = Field("unstruct_event_myvendor_bad_schema_evolution_1", expectedStruct, Nullable, Set.empty)

      val recoveryField1 =
        Field("unstruct_event_myvendor_bad_schema_evolution_1_recovered_1_0_1_e7cf2565", recoveryStruct1, Nullable, Set.empty)
      val recoveryField2 =
        Field("unstruct_event_myvendor_bad_schema_evolution_1_recovered_1_0_2_cacf6738", recoveryStruct2, Nullable, Set.empty)

      TypedTabledEntity(
        tabledEntity,
        expectedField,
        Set((0, 0), (0, 3)),
        List(((0, 1), recoveryField1), ((0, 2), recoveryField2))
      )
    }

    NonAtomicFields.resolveTypes(embeddedResolver, input).map { case NonAtomicFields.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields.head must beEqualTo(expected))
    }

  }

  def fail1 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 9))
    )

    NonAtomicFields.resolveTypes(embeddedResolver, input).map { case NonAtomicFields.Result(fields, failures) =>
      (fields must beEmpty) and
        (failures must haveSize(1)) and
        (failures.head must beLike { case failure: NonAtomicFields.ColumnFailure =>
          (failure.tabledEntity must beEqualTo(tabledEntity)) and
            (failure.versionsInBatch must beEqualTo(Set((0, 9)))) and
            (failure.failure must beLike { case _: FailureDetails.LoaderIgluError.IgluError => ok })
        })
    }

  }

  def fail2 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "doesnotexist", 1)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    NonAtomicFields.resolveTypes(embeddedResolver, input).map { case NonAtomicFields.Result(fields, failures) =>
      (fields must beEmpty) and
        (failures must haveSize(1)) and
        (failures.head must beLike { case failure: NonAtomicFields.ColumnFailure =>
          (failure.tabledEntity must beEqualTo(tabledEntity)) and
            (failure.versionsInBatch must beEqualTo(Set((0, 0)))) and
            (failure.failure must beLike { case _: FailureDetails.LoaderIgluError.SchemaListNotFound => ok })
        })
    }

  }

  def fail3 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "invalid_syntax", 1)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    NonAtomicFields.resolveTypes(embeddedResolver, input).map { case NonAtomicFields.Result(fields, failures) =>
      (fields must beEmpty) and
        (failures must haveSize(1)) and
        (failures.head must beLike { case failure: NonAtomicFields.ColumnFailure =>
          (failure.tabledEntity must beEqualTo(tabledEntity)) and
            (failure.versionsInBatch must beEqualTo(Set((0, 0)))) and
            (failure.failure must beLike { case _: FailureDetails.LoaderIgluError.InvalidSchema => ok })
        })
    }

  }

}

object NonAtomicFieldsSpec {

  // A resolver that resolves embedded schemas only
  val embeddedResolver = Resolver[IO](Nil, None)
}
