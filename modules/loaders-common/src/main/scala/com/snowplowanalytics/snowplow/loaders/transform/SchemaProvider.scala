/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import cats.data.{EitherT, NonEmptyVector}
import cats.effect.Sync
import cats.implicits._
import com.snowplowanalytics.iglu.client.{ClientError, Resolver}
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaMap, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits.toSchema
import com.snowplowanalytics.snowplow.badrows.FailureDetails

import scala.math.Ordered._

private[transform] object SchemaProvider {

  private def getSchema[F[_]: Sync: RegistryLookup](
    resolver: Resolver[F],
    schemaKey: SchemaKey
  ): EitherT[F, FailureDetails.LoaderIgluError, SelfDescribingSchema[Schema]] =
    for {
      json <- EitherT(resolver.lookupSchema(schemaKey))
                .leftMap(resolverBadRow(schemaKey))
      schema <- EitherT.fromOption[F](Schema.parse(json), parseSchemaBadRow(schemaKey))
    } yield SelfDescribingSchema(SchemaMap(schemaKey), schema)

  // Note schema order of the returned list is not guaranteed
  def fetchSchemasWithSameModel[F[_]: Sync: RegistryLookup](
    resolver: Resolver[F],
    schemaKey: SchemaKey
  ): EitherT[F, FailureDetails.LoaderIgluError, NonEmptyVector[SelfDescribingSchema[Schema]]] =
    for {
      schemaKeys <- EitherT(resolver.listSchemasLike(schemaKey))
                      .leftMap(resolverFetchBadRow(schemaKey.vendor, schemaKey.name, schemaKey.format, schemaKey.version.model))
                      .map(_.schemas.toVector)
      schemaKeys <- EitherT.rightT[F, FailureDetails.LoaderIgluError](schemaKeys.filter(_ < schemaKey))
      topSchema <- getSchema(resolver, schemaKey)
      lowerSchemas <- schemaKeys.filter(_ < schemaKey).traverse(getSchema(resolver, _))
    } yield NonEmptyVector(topSchema, lowerSchemas)

  private def resolverFetchBadRow(
    vendor: String,
    name: String,
    format: String,
    model: Int
  )(
    e: ClientError.ResolutionError
  ): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.SchemaListNotFound(SchemaCriterion(vendor = vendor, name = name, format = format, model = model), e)

  private def resolverBadRow(schemaKey: SchemaKey)(e: ClientError.ResolutionError): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.IgluError(schemaKey, e)

  private def parseSchemaBadRow(schemaKey: SchemaKey): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.InvalidSchema(schemaKey, "Cannot be parsed as JSON Schema AST")

}
