/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import cats.effect.Sync
import cats.implicits._
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.snowplow.badrows.FailureDetails

object NonAtomicFields {

  /**
   * Describes the Field Types (not data) present in a batch of events
   *
   * @param fields
   *   field type information about each family of Iglu schema. E.g. if a batch contains versions
   *   1-0-0, 1-0-1 and 1-1-0 of a schema, they will be present as a single item of this list. If
   *   the batch also contains version 2-0-0 of that schema, it will be present as an extra item of
   *   this list.
   * @param igluFailures
   *   details of schemas that were present in the batch but could not be looked up by the Iglu
   *   resolver.
   */
  case class Result(fields: List[TypedTabledEntity], igluFailures: List[ColumnFailure])

  /**
   * Describes a failure to lookup a series of Iglu schemas
   *
   * @param tabledEntity
   *   The family of iglu schemas for which the lookup was needed
   * @param versionsInBatch
   *   The schema versions for which a lookup was needed
   * @param failure
   *   Why the lookup failed
   */
  case class ColumnFailure(
    tabledEntity: TabledEntity,
    versionsInBatch: Set[SchemaSubVersion],
    failure: FailureDetails.LoaderIgluError
  )

  def resolveTypes[F[_]: Sync: RegistryLookup](
    resolver: Resolver[F],
    entities: Map[TabledEntity, Set[SchemaSubVersion]],
    filterCriteria: List[SchemaCriterion]
  ): F[Result] =
    entities.toList
      .map { case (tabledEntity, subVersions) =>
        // First phase of entity filtering, before we fetch schemas from Iglu and create `TypedTabledEntity`.
        // If all sub-versions are filtered out, whole family is removed.
        tabledEntity -> filterSubVersions(filterCriteria, tabledEntity, subVersions)
      }
      .filter { case (_, subVersions) =>
        // Remove whole schema family if there is no subversion left after filtering
        subVersions.nonEmpty
      }
      .traverse { case (tabledEntity, subVersions) =>
        SchemaProvider
          .fetchSchemasWithSameModel(resolver, TabledEntity.toSchemaKey(tabledEntity, subVersions.max))
          .map(TypedTabledEntity.build(tabledEntity, subVersions, _))
          // Second phase of entity filtering.
          // We can't do it sooner based on a result of `fetchSchemasWithSameModel` because we can't have 'holes' in Iglu schema family when building typed entities.
          // Otherwise we may end up with invalid and incompatible merged schema model.
          // Here `TypedTabledEntity` is already properly created using contiguous series of schemas, so we can try to skip some sub-versions.
          .map { typedTabledEntity =>
            val filteredSubVersions = filterSubVersions(filterCriteria, typedTabledEntity.tabledEntity, typedTabledEntity.mergedVersions)
            typedTabledEntity.copy(mergedVersions = filteredSubVersions)
          }
          .leftMap(ColumnFailure(tabledEntity, subVersions, _))
          .value
      }
      .map { eithers =>
        val (failures, good) = eithers.separate
        Result(good, failures)
      }

  private def filterSubVersions(
    filterCriteria: List[SchemaCriterion],
    tabledEntity: TabledEntity,
    subVersions: Set[SchemaSubVersion]
  ): Set[SchemaSubVersion] =
    if (filterCriteria.nonEmpty) {
      subVersions
        .filter { subVersion =>
          val schemaKey = TabledEntity.toSchemaKey(tabledEntity, subVersion)
          doesNotMatchCriteria(filterCriteria, schemaKey)
        }
    } else {
      subVersions
    }

  private def doesNotMatchCriteria(filterCriteria: List[SchemaCriterion], schemaKey: SchemaKey): Boolean =
    !filterCriteria.exists(_.matches(schemaKey))
}
