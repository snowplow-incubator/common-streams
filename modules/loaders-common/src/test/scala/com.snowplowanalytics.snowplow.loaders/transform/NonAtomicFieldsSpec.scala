/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import scala.collection.immutable.ListSet
import cats.effect.IO
import cats.data.NonEmptyVector
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import com.snowplowanalytics.iglu.core.SchemaCriterion
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
      return nothing for the Iglu Central ad_break_end_event schema $ue4
      return a JSON field for the Iglu Central anything-a schema $ue5
      return a field prefixed with underscore if field starts with a digit $ueDigit
      return a merged schema if the batch has a schema with empty properties and additionalProperties=false $emptyProp
      return a merged struct with field order prioritizing original fields before new additions $fieldOrder

    when resolving for known schemas in contexts should
      return an un-merged schema if the batch uses the first schema in a series $c1
      return a merged schema if the batch uses the last schema in a series $c2
      return a merged schema if the batch uses all schemas in a series $c3
      return nothing for the Iglu Central ad_break_end_event schema $c4
      return a JSON field for the Iglu Central anything-a schema $c5

    when resolving for known schema in contexts and unstruct_event should
      return separate entity for the context and the unstruct_event $both1

    when resolving for schemas in a series that violates schema evolution rules should
      return no recovery schemas if the batch uses the first schema in the series $recovery1
      return recovery schemas if the batch uses schemas that broke the rules $recovery2
      return recovery schemas and merged base schema if the batch uses schemas that broke the rules and those that obeyed rules $recovery3

    when handling Iglu failures should
      return a IgluError if schema key does not exist in a valid series of schemas $fail1
      return an InvalidSchema if the series contains a schema that cannot be parsed $fail2

    when resolving schemas with clashing field names should
      return Left(ClashingFieldNames) $clashingNames1
      return Left(ClashingFieldNames) if there are more than one pair of fields with clashing names $clashingNames2
      return Right if one of the schemas with clashing names are skipped $clashingNames3

    ClashingFieldNames exception message should
      contain all the field pairs that are given $clashingNamesException1
  """

  def ue1 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
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

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(1)) and
          (fields.head must beEqualTo(expected))
      }
    }

  }

  def ue2 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((1, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          Field("col_a", Type.String, Required),
          Field("col_b", Type.String, Nullable),
          Field("col_c", Type.String, Nullable)
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

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(1)) and
          (fields.head must beEqualTo(expected))
      }
    }

  }

  def ue3 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 0), (0, 1), (1, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          Field("col_a", Type.String, Required),
          Field("col_b", Type.String, Nullable),
          Field("col_c", Type.String, Nullable)
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

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(1)) and
          (fields.head must beEqualTo(expected))
      }
    }

  }

  def ue4 = {

    // Example of a schema which is an empty object with no additional properties
    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "com.snowplowanalytics.snowplow.media", "ad_break_end_event", 1)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must beEmpty)
      }
    }

  }

  def ue5 = {

    // Example of a permissive schema which permits any JSON
    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "com.snowplowanalytics.iglu", "anything-a", 1)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expected = {
      val expectedType  = Type.Json
      val expectedField = Field("unstruct_event_com_snowplowanalytics_iglu_anything_a_1", expectedType, Nullable, Set.empty)

      TypedTabledEntity(
        tabledEntity,
        expectedField,
        Set((0, 0)),
        Nil
      )
    }

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(1)) and
          (fields.head must beEqualTo(expected))
      }
    }

  }

  def ueDigit = {
    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "digit", 1)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          Field("_1col_a", Type.String, Required).copy(accessors = Set("1col_a"))
        )
      )

      val expectedField = Field("unstruct_event_myvendor_digit_1", expectedStruct, Nullable, Set.empty)

      TypedTabledEntity(
        tabledEntity,
        expectedField,
        Set((0, 0)),
        Nil
      )
    }

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(1)) and
          (fields.head must beEqualTo(expected))
      }
    }
  }

  def emptyProp = {
    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "test_empty_prop", 1)

    val input = Map(
      tabledEntity -> Set((0, 1), (0, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          Field("my_string", Type.String, Nullable).copy(accessors = Set("myString"))
        )
      )

      val expectedField = Field("unstruct_event_myvendor_test_empty_prop_1", expectedStruct, Nullable, Set.empty)

      TypedTabledEntity(
        tabledEntity,
        expectedField,
        Set((0, 1)),
        Nil
      )
    }

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(1)) and
          (fields.head must beEqualTo(expected))
      }
    }
  }

  def fieldOrder = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 10)

    val input = Map(
      tabledEntity -> Set((0, 0), (0, 1))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          // original fields
          Field("col_m", Type.String, Nullable),
          Field("col_n", Type.String, Nullable),
          // newly added fields
          Field("col_a", Type.String, Nullable), // earlier alphabetically
          Field("col_z", Type.String, Nullable) // later alphabetically
        )
      )

      val expectedField = Field("unstruct_event_myvendor_myschema_10", expectedStruct, Nullable, Set.empty)

      TypedTabledEntity(
        tabledEntity,
        expectedField,
        Set((0, 0), (0, 1)),
        Nil
      )
    }

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(1)) and
          (fields.head must beEqualTo(expected))
      }
    }

  }

  def c1 = {

    val tabledEntity = TabledEntity(TabledEntity.Context, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
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

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(1)) and
          (fields.head must beEqualTo(expected))
      }
    }

  }

  def c2 = {

    val tabledEntity = TabledEntity(TabledEntity.Context, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((1, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          Field("_schema_version", Type.String, Required),
          Field("col_a", Type.String, Required),
          Field("col_b", Type.String, Nullable),
          Field("col_c", Type.String, Nullable)
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

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(1)) and
          (fields.head must beEqualTo(expected))
      }
    }

  }

  def c3 = {

    val tabledEntity = TabledEntity(TabledEntity.Context, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 0), (0, 1), (1, 0))
    )

    val expected = {

      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          Field("_schema_version", Type.String, Required),
          Field("col_a", Type.String, Required),
          Field("col_b", Type.String, Nullable),
          Field("col_c", Type.String, Nullable)
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

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(1)) and
          (fields.head must beEqualTo(expected))
      }
    }

  }

  def c4 = {

    // Example of a schema which is an empty object with no additional properties
    val tabledEntity = TabledEntity(TabledEntity.Context, "com.snowplowanalytics.snowplow.media", "ad_break_end_event", 1)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must beEmpty)
      }
    }

  }

  def c5 = {

    // Example of a permissive schema which permits any JSON
    val tabledEntity = TabledEntity(TabledEntity.Context, "com.snowplowanalytics.iglu", "anything-a", 1)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expected = {

      val expectedType = Type.Json

      val expectedArray = Type.Array(expectedType, Nullable)

      val expectedField = Field("contexts_com_snowplowanalytics_iglu_anything_a_1", expectedArray, Nullable, Set.empty)

      TypedTabledEntity(
        tabledEntity,
        expectedField,
        Set((0, 0)),
        Nil
      )
    }

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(1)) and
          (fields.head must beEqualTo(expected))
      }
    }

  }

  def both1 = {

    val tabledEntity1 = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)
    val tabledEntity2 = TabledEntity(TabledEntity.Context, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity1 -> Set((0, 0)),
      tabledEntity2 -> Set((0, 0))
    )

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(2)) and
          (fields.map(_.tabledEntity) must contain(allOf(tabledEntity1, tabledEntity2)))
      }
    }

  }

  def recovery1 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "bad_schema_evolution", 1)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
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

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(1)) and
          (fields.head must beEqualTo(expected))
      }
    }

  }

  def recovery2 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "bad_schema_evolution", 1)

    val input = Map(
      tabledEntity -> Set((0, 0), (0, 1), (0, 2))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          Field("col_a", Type.String, Required)
        )
      )

      val recoveryStruct1 = Type.Struct(
        NonEmptyVector.of(
          Field("col_a", Type.Double, Required)
        )
      )

      val recoveryStruct2 = Type.Struct(
        NonEmptyVector.of(
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

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(1)) and
          (fields.head must beEqualTo(expected))
      }
    }

  }

  def recovery3 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "bad_schema_evolution", 1)

    val input = Map(
      tabledEntity -> Set((0, 0), (0, 1), (0, 2), (0, 3))
    )

    val expected = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          Field("col_a", Type.String, Required),
          Field("col_b", Type.Long, Nullable)
        )
      )

      val recoveryStruct1 = Type.Struct(
        NonEmptyVector.of(
          Field("col_a", Type.Double, Required)
        )
      )

      val recoveryStruct2 = Type.Struct(
        NonEmptyVector.of(
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

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(1)) and
          (fields.head must beEqualTo(expected))
      }
    }

  }

  def fail1 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)

    val input = Map(
      tabledEntity -> Set((0, 9))
    )

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (fields must beEmpty) and
          (failures must haveSize(1)) and
          (failures.head must beLike { case failure: NonAtomicFields.ColumnFailure =>
            (failure.tabledEntity must beEqualTo(tabledEntity)) and
              (failure.versionsInBatch must beEqualTo(Set((0, 9)))) and
              (failure.failure must beLike { case _: FailureDetails.LoaderIgluError.IgluError => ok })
          })
      }
    }

  }

  def fail2 = {

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "invalid_syntax", 1)

    val input = Map(
      tabledEntity -> Set((0, 0))
    )

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
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

  def clashingNames1 = {

    val tabledEntity1 = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "a.b.c", 1)
    val tabledEntity2 = tabledEntity1.copy(schemaName = "a_b.c")

    val input = Map(
      tabledEntity1 -> Set((0, 0)),
      tabledEntity2 -> Set((0, 0))
    )

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beLeft(
        NonAtomicFields.ResolveTypesException.ClashingFieldNames(Set(Set(tabledEntity1, tabledEntity2)))
      )
    }

  }

  def clashingNames2 = {

    val tabledEntity11 = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "a.b.c", 1)
    val tabledEntity12 = tabledEntity11.copy(schemaName = "a_b.c")
    val tabledEntity13 = tabledEntity11.copy(schemaName = "a_b_c")

    val tabledEntity21 = tabledEntity11.copy(schemaName = "d.e.f")
    val tabledEntity22 = tabledEntity11.copy(schemaName = "d_e_f")

    val input = Map(
      tabledEntity11 -> Set((0, 0)),
      tabledEntity12 -> Set((0, 0)),
      tabledEntity13 -> Set((0, 0)),
      tabledEntity21 -> Set((0, 0)),
      tabledEntity22 -> Set((0, 0))
    )

    NonAtomicFields.resolveTypes(embeddedResolver, input, List.empty).map {
      _ must beLeft(
        NonAtomicFields.ResolveTypesException.ClashingFieldNames(
          Set(
            Set(tabledEntity21, tabledEntity22),
            Set(tabledEntity11, tabledEntity12, tabledEntity13)
          )
        )
      )
    }

  }

  def clashingNames3 = {

    val tabledEntity1 = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "a.b.c", 1)
    val tabledEntity2 = tabledEntity1.copy(schemaName = "a_b.c")

    val input = Map(
      tabledEntity1 -> Set((0, 0)),
      tabledEntity2 -> Set((0, 0))
    )

    val skipped = List(
      SchemaCriterion("myvendor", "a.b.c", "jsonschema", 1)
    )

    val expected = {
      val expectedStruct = Type.Struct(
        NonEmptyVector.of(
          Field("col_a", Type.String, Required)
        )
      )

      val expectedField = Field("unstruct_event_myvendor_a_b_c_1", expectedStruct, Nullable, Set.empty)

      TypedTabledEntity(
        tabledEntity2,
        expectedField,
        Set((0, 0)),
        Nil
      )
    }

    NonAtomicFields.resolveTypes(embeddedResolver, input, skipped).map {
      _ must beRight.like { case NonAtomicFields.Result(fields, failures) =>
        (failures must beEmpty) and
          (fields must haveSize(1)) and
          (fields.head must beEqualTo(expected))
      }
    }

  }

  def clashingNamesException1 = {

    val tabledEntity1 = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "s1", 1)
    val tabledEntity2 = tabledEntity1.copy(schemaName = "s2")
    val tabledEntity3 = tabledEntity1.copy(schemaName = "s3")

    NonAtomicFields.ResolveTypesException
      .ClashingFieldNames(
        ListSet(
          ListSet(tabledEntity1, tabledEntity2),
          ListSet(tabledEntity1, tabledEntity2, tabledEntity3)
        )
      )
      .getMessage must beEqualTo(
      "schemas [myvendor.s1, myvendor.s2], [myvendor.s1, myvendor.s2, myvendor.s3] have clashing column names"
    )

  }

}

object NonAtomicFieldsSpec {

  // A resolver that resolves embedded schemas only
  val embeddedResolver = Resolver[IO](Nil, None)
}
