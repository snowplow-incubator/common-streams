/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import cats.data.NonEmptyVector
import cats.effect.IO
import io.circe.parser.{parse => parseToJson}
import scala.io.Source

import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaMap, SelfDescribingSchema}
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.JavaNetRegistryLookup._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits.toSchema
import com.snowplowanalytics.snowplow.badrows.FailureDetails

class SchemaProviderSpec extends Specification with CatsEffect {

  import SchemaProviderSpec._

  def is = s2"""
  SchemaProvider fetchSchemasWithSameModel should
    fetch a single schema from a series when queried with the lowest schema key $e1
    fetch all schemas from a series when queried with the highest schema key $e2
    fetch subset of schemas from a series when queried with an intermediate schema key $e3
    return a IgluError if schema key does not exist in a valid series of schemas $e4
    return a SchemaListNotFound if the series of schemas does not exist $e5
    return an InvalidSchema if the series contains a schema that cannot be parsed $e6
  """

  def e1 = {

    val expected = NonEmptyVector.of(
      SelfDescribingSchema(SchemaMap(testSchemaKey700), testSchema700)
    )

    SchemaProvider.fetchSchemasWithSameModel(embeddedResolver, testSchemaKey700).value.map { result =>
      result must beRight { schemas: NonEmptyVector[SelfDescribingSchema[Schema]] =>
        schemas must beEqualTo(expected)
      }
    }

  }

  def e2 = {

    val expected = Vector(
      SelfDescribingSchema(SchemaMap(testSchemaKey700), testSchema700),
      SelfDescribingSchema(SchemaMap(testSchemaKey701), testSchema701),
      SelfDescribingSchema(SchemaMap(testSchemaKey710), testSchema710)
    )

    SchemaProvider.fetchSchemasWithSameModel(embeddedResolver, testSchemaKey710).value.map { result =>
      result must beRight { schemas: NonEmptyVector[SelfDescribingSchema[Schema]] =>
        schemas.toVector must containTheSameElementsAs(expected)
      }
    }

  }

  def e3 = {

    val expected = Vector(
      SelfDescribingSchema(SchemaMap(testSchemaKey700), testSchema700),
      SelfDescribingSchema(SchemaMap(testSchemaKey701), testSchema701)
    )

    SchemaProvider.fetchSchemasWithSameModel(embeddedResolver, testSchemaKey701).value.map { result =>
      result must beRight { schemas: NonEmptyVector[SelfDescribingSchema[Schema]] =>
        schemas.toVector must containTheSameElementsAs(expected)
      }
    }

  }

  def e4 =
    SchemaProvider.fetchSchemasWithSameModel(embeddedResolver, testSchemaKey702).value.map { result =>
      result must beLeft.like { case failure: FailureDetails.LoaderIgluError.IgluError =>
        failure.schemaKey must beEqualTo(testSchemaKey702)
      }
    }

  def e5 = {

    val testSchemaKey = SchemaKey.fromUri("iglu:myvendor/doesnotexist/jsonschema/7-0-0").toOption.get
    val criterion     = SchemaCriterion.parse("iglu:myvendor/doesnotexist/jsonschema/7-*-*").get

    SchemaProvider.fetchSchemasWithSameModel(embeddedResolver, testSchemaKey).value.map { result =>
      result must beLeft.like { case failure: FailureDetails.LoaderIgluError.SchemaListNotFound =>
        failure.schemaCriterion must beEqualTo(criterion)
      }
    }

  }

  def e6 = {

    val testSchemaKey = SchemaKey.fromUri("iglu:myvendor/invalid_syntax/jsonschema/1-0-0").toOption.get

    SchemaProvider.fetchSchemasWithSameModel(embeddedResolver, testSchemaKey).value.map { result =>
      result must beLeft.like { case failure: FailureDetails.LoaderIgluError.InvalidSchema =>
        failure.schemaKey must beEqualTo(testSchemaKey)
      }
    }

  }

}

object SchemaProviderSpec {

  // A resolver that resolves embedded schemas only
  val embeddedResolver = Resolver[IO](Nil, None)

  val testSchemaKey700: SchemaKey = SchemaKey.fromUri("iglu:myvendor/myschema/jsonschema/7-0-0").toOption.get
  val testSchemaKey701: SchemaKey = SchemaKey.fromUri("iglu:myvendor/myschema/jsonschema/7-0-1").toOption.get
  val testSchemaKey702: SchemaKey = SchemaKey.fromUri("iglu:myvendor/myschema/jsonschema/7-0-2").toOption.get // This one does not exist
  val testSchemaKey710: SchemaKey = SchemaKey.fromUri("iglu:myvendor/myschema/jsonschema/7-1-0").toOption.get
  val testSchemaKey800: SchemaKey = SchemaKey.fromUri("iglu:myvendor/myschema/jsonschema/8-0-0").toOption.get

  def unsafeParseSchema(resource: String): Schema = {
    val text = Source.fromResource(resource).mkString
    val json = parseToJson(text).toOption.get
    Schema.parse(json).get
  }

  val testSchema700: Schema = unsafeParseSchema("iglu-client-embedded/schemas/myvendor/myschema/jsonschema/7-0-0")
  val testSchema701: Schema = unsafeParseSchema("iglu-client-embedded/schemas/myvendor/myschema/jsonschema/7-0-1")
  val testSchema710: Schema = unsafeParseSchema("iglu-client-embedded/schemas/myvendor/myschema/jsonschema/7-1-0")
  val testSchema800: Schema = unsafeParseSchema("iglu-client-embedded/schemas/myvendor/myschema/jsonschema/8-0-0")

}
