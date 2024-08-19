/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import cats.data.{EitherT, NonEmptyList}
import cats.effect.Sync
import com.snowplowanalytics.iglu.client.{ClientError, Resolver}
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.resolver.Resolver.SchemaResolutionError
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits.toSchema
import com.snowplowanalytics.snowplow.badrows.FailureDetails

private[transform] object SchemaProvider {

  // Note schema order of the returned list is guaranteed
  def fetchSchemasWithSameModel[F[_]: Sync: RegistryLookup](
    resolver: Resolver[F],
    schemaKey: SchemaKey
  ): EitherT[F, FailureDetails.LoaderIgluError, NonEmptyList[SelfDescribingSchema[Schema]]] =
    for {
      jsons <- EitherT(resolver.lookupSchemasUntil(schemaKey))
                 .leftMap { case SchemaResolutionError(schemaKey, error) => resolverBadRow(schemaKey)(error) }
      schemas <- jsons.traverse { json =>
                   EitherT
                     .fromOption[F](Schema.parse(json.schema), parseSchemaBadRow(json.self.schemaKey))
                     .map(schema => SelfDescribingSchema(SchemaMap(json.self.schemaKey), schema))
                 }
    } yield schemas

  private def resolverBadRow(schemaKey: SchemaKey)(e: ClientError.ResolutionError): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.IgluError(schemaKey, e)

  private def parseSchemaBadRow(schemaKey: SchemaKey): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.InvalidSchema(schemaKey, "Cannot be parsed as JSON Schema AST")

}
