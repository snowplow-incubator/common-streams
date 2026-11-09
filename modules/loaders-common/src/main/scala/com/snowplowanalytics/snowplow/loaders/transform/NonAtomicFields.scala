/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import cats.data.NonEmptyList
import cats.effect.Async
import cats.effect.implicits._
import cats.implicits._
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
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
   *   resolver. An entity can appear in both `fields` and `igluFailures`: if some sub-versions of a
   *   family were fetched but others in the batch were not, the family's type is built from the
   *   fetched schemas and each missing sub-version is reported here.
   */
  case class Result(fields: Vector[TypedTabledEntity], igluFailures: List[ColumnFailure])

  /**
   * Describes a failure to lookup a series of Iglu schemas
   *
   * @param tabledEntity
   *   The family of iglu schemas for which the lookup was needed
   * @param versionsInBatch
   *   The exact set of schema versions that failed. Not necessarily every version of the family
   *   present in the batch.
   * @param failure
   *   Why the lookup failed
   */
  case class ColumnFailure(
    tabledEntity: TabledEntity,
    versionsInBatch: Set[SchemaSubVersion],
    failure: FailureDetails.LoaderIgluError
  )

  sealed trait ResolveTypesException extends Throwable
  object ResolveTypesException {
    case class ClashingFieldNames(fieldsWithSameNames: Set[Set[TabledEntity]]) extends ResolveTypesException {
      override def getMessage: String = {
        val errorMessageSchemas = fieldsWithSameNames
          .map { entities =>
            val schemaListStr = entities.map(e => s"${e.vendor}.${e.schemaName}").mkString(", ")
            s"[$schemaListStr]"
          }
          .mkString(", ")
        s"schemas $errorMessageSchemas have clashing column names"
      }
    }
  }

  def resolveTypes[F[_]: Async: RegistryLookup](
    resolver: Resolver[F],
    entities: Map[TabledEntity, Set[SchemaSubVersion]],
    filterCriteria: List[SchemaCriterion]
  ): F[Either[ResolveTypesException, Result]] =
    entities.toVector
      .map { case (tabledEntity, subVersions) =>
        // First phase of entity filtering, before we fetch schemas from Iglu and create `TypedTabledEntity`.
        // If all sub-versions are filtered out, whole family is removed.
        tabledEntity -> filterSubVersions(filterCriteria, tabledEntity, subVersions)
      }
      .filter { case (_, subVersions) =>
        // Remove whole schema family if there is no subversion left after filtering
        subVersions.nonEmpty
      }
      .parTraverse { case (tabledEntity, subVersions) =>
        SchemaProvider
          .fetchSchemasWithSameModel(resolver, TabledEntity.toSchemaKey(tabledEntity, subVersions.max))
          .semiflatMap { schemas =>
            missingVersionFailures(resolver, tabledEntity, subVersions, schemas).map { missingFailures =>
              val typedTabledEntity = TypedTabledEntity
                .build(tabledEntity, subVersions, schemas)
                // Second phase of entity filtering.
                // We can't do it sooner based on a result of `fetchSchemasWithSameModel` because we can't have 'holes' in Iglu schema family when building typed entities.
                // Otherwise we may end up with invalid and incompatible merged schema model.
                // Here `TypedTabledEntity` is already properly created using contiguous series of schemas, so we can try to skip some sub-versions.
                .map { typedTabledEntity =>
                  val filteredSubVersions =
                    filterSubVersions(filterCriteria, typedTabledEntity.tabledEntity, typedTabledEntity.mergedVersions)
                  typedTabledEntity.copy(mergedVersions = filteredSubVersions)
                }
              (missingFailures, typedTabledEntity)
            }
          }
          .leftMap(ColumnFailure(tabledEntity, subVersions, _))
          .value
      }
      .map { eithers =>
        val (failures, good)    = eithers.separate
        val fields              = good.flatMap(_._2)
        val allFailures         = failures.toList ::: good.toList.flatMap(_._1)
        val fieldsWithSameNames = findFieldsWithSameNames(fields)
        if (fieldsWithSameNames.isEmpty) {
          Right(Result(fields, allFailures))
        } else {
          Left(ResolveTypesException.ClashingFieldNames(fieldsWithSameNames))
        }
      }

  /**
   * Reports the sub-versions that were present in the batch but not among the fetched schemas
   *
   * `lookupSchemasUntil` walks the family in order. When an addition is not found, it jumps to the
   * next revision, so it skips that addition *and every later addition of the same revision*, even
   * ones that exist. So "requested but not fetched" does not mean "doesn't exist":
   *   - A version that does not exist (or could not be fetched) is reported with the resolver's
   *     error.
   *   - A version that exists but was skipped can't be merged, because the family can't have a hole
   *     in it. It is reported with a message naming the first missing version of its revision,
   *     which is the root cause.
   *
   * `Transform` writes a null column for any version the family's type does not cover, so every
   * such version must be reported here for its events to become failed events.
   */
  private def missingVersionFailures[F[_]: Async: RegistryLookup](
    resolver: Resolver[F],
    tabledEntity: TabledEntity,
    subVersions: Set[SchemaSubVersion],
    schemas: NonEmptyList[SelfDescribingSchema[Schema]]
  ): F[List[ColumnFailure]] = {
    val fetched = schemas.toList.map(sds => TypedTabledEntity.keyToSubVersion(sds.self.schemaKey)).toSet
    (subVersions -- fetched).toList.sorted.traverse { subVersion =>
      val schemaKey = TabledEntity.toSchemaKey(tabledEntity, subVersion)
      resolver.lookupSchema(schemaKey).map {
        case Left(error) =>
          ColumnFailure(tabledEntity, Set(subVersion), FailureDetails.LoaderIgluError.IgluError(schemaKey, error))
        case Right(_) =>
          // An edge case: this version exists, but the family lookup did not reach it, which needs a hole
          // in the registry or a schema published since. There is no ClientError to report, because
          // lookupSchemasUntil discards the error that stopped its walk, so for this rare case we accept
          // InvalidSchema, the only LoaderIgluError variant that carries a message, though the schema
          // itself is not invalid.
          val (revision, _)        = subVersion
          val firstMissingAddition = Iterator.from(0).dropWhile(a => fetched.contains((revision, a))).next()
          val firstMissingKey      = TabledEntity.toSchemaKey(tabledEntity, (revision, firstMissingAddition))
          val message =
            s"The lookup of its family of schemas did not get past ${firstMissingKey.version.asString}, so this version was not resolved and cannot be merged"
          ColumnFailure(tabledEntity, Set(subVersion), FailureDetails.LoaderIgluError.InvalidSchema(schemaKey, message))
      }
    }
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

  private def findFieldsWithSameNames(fields: Vector[TypedTabledEntity]): Set[Set[TabledEntity]] =
    fields
      .groupBy(_.mergedField.name)
      .values
      .filter(_.size > 1)
      .map(_.map(_.tabledEntity).toSet)
      .toSet
}
