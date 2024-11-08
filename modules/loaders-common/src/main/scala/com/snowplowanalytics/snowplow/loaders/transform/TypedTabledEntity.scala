/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders.transform

import cats.data.{NonEmptyList, NonEmptyVector}
import cats.implicits._
import io.circe.syntax._
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Migrations, Type}
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Data => SdkData, SnowplowEvent}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._

/**
 * Field type information for a family of versioned Iglu schemas which are treated as a common
 * entity when writing to the table
 *
 * E.g. unstruct events with types 1-0-0, 1-0-1, and 1-1-0 are merged into the same
 * TypedTabledEntity
 *
 * @param tabledEntity
 *   Identifier to this entity. Includes meta data but no type information.
 * @param mergedField
 *   The schema-ddl Field describing a merge of all schema versions in this group
 * @param mergedVersions
 *   The sub-versions (e.g. '*-0-0' and '*-0-1') which were successfully merged into the mergedField
 * @param recoveries
 *   The schema-ddl Fields for schema versions which could not be merged into the main mergedField
 */
case class TypedTabledEntity(
  tabledEntity: TabledEntity,
  mergedField: Field,
  mergedVersions: Set[SchemaSubVersion],
  recoveries: List[(SchemaSubVersion, Field)]
)

object TypedTabledEntity {

  /**
   * Calculate the TypedTableEntity for a group of entities
   *
   * This is a pure function: we have already looked up schemas from Iglu.
   *
   * @param tabledEntity
   *   Identifier to this entity
   * @param subVersions
   *   Sub-versions (e.g. '*-0-0') that were present in the batch of events.
   * @param schemas
   *   Iglu schemas pre-fetched from Iglu Server ordered by key
   */
  private[transform] def build(
    tabledEntity: TabledEntity,
    subVersions: Set[SchemaSubVersion],
    schemas: NonEmptyList[SelfDescribingSchema[Schema]]
  ): Option[TypedTabledEntity] =
    schemas.toList
      .flatMap { sds =>
        fieldFromSchema(tabledEntity, sds.schema).map((_, sds))
      }
      .toNel
      .map { nel =>
        val (rootField, rootSchema) = nel.head
        val tte                     = TypedTabledEntity(tabledEntity, rootField, Set(keyToSubVersion(rootSchema.self.schemaKey)), Nil)
        nel.tail
          .foldLeft(tte) { case (columnGroup, (field, selfDescribingSchema)) =>
            val schemaKey  = selfDescribingSchema.self.schemaKey
            val subversion = keyToSubVersion(schemaKey)
            Migrations.mergeSchemas(columnGroup.mergedField, field) match {
              case Left(_) =>
                if (subVersions.contains(subversion)) {
                  val hash = "%08x".format(selfDescribingSchema.schema.asJson.noSpaces.hashCode())
                  // typedField always has a single element in matchingKeys
                  val recoverPoint = schemaKey.version.asString.replaceAll("-", "_")
                  val newName      = s"${field.name}_recovered_${recoverPoint}_$hash"
                  columnGroup.copy(recoveries = (subversion -> field.copy(name = newName)) :: columnGroup.recoveries)
                } else {
                  // do not create a recovered column if that type were not in the batch
                  columnGroup
                }
              case Right(mergedField) =>
                columnGroup.copy(mergedField = mergedField, mergedVersions = columnGroup.mergedVersions + subversion)
            }
          }
      }
      .map { tte =>
        tte.copy(recoveries = tte.recoveries.reverse)
      }

  private def fieldFromSchema(tabledEntity: TabledEntity, schema: Schema): Option[Field] = {
    val sdkEntityType = tabledEntity.entityType match {
      case TabledEntity.UnstructEvent => SdkData.UnstructEvent
      case TabledEntity.Context       => SdkData.Contexts(SdkData.CustomContexts)
    }
    val fieldName = SnowplowEvent.transformSchema(sdkEntityType, tabledEntity.vendor, tabledEntity.schemaName, tabledEntity.model)

    val field = tabledEntity.entityType match {
      case TabledEntity.UnstructEvent =>
        Field.build(fieldName, schema, enforceValuePresence = false).map(Field.normalize(_))
      case TabledEntity.Context =>
        // Must normalize first and add the schema key field after. To avoid unlikely weird issues
        // with similar existing keys.
        Field
          .buildRepeated(fieldName, schema, enforceItemPresence = true, Type.Nullability.Nullable)
          .map(Field.normalize(_))
          .map(addSchemaVersionKey(_))
    }

    // Accessors are meaningless for a schema's top-level field
    field.map(_.copy(accessors = Set.empty))
  }

  private def keyToSubVersion(key: SchemaKey): SchemaSubVersion = (key.version.revision, key.version.addition)

  private def addSchemaVersionKey(field: Field): Field = {
    val fieldType = field.fieldType match {
      case arr @ Type.Array(struct @ Type.Struct(subFields), _) =>
        val head = Field("_schema_version", Type.String, Type.Nullability.Required)
        val tail = subFields.filter( // Our special key takes priority over a key of the same name in the schema
          _.name =!= "_schema_version"
        )
        arr.copy(element = struct.copy(fields = NonEmptyVector(head, tail)))
      case other =>
        // This is OK. It must be a weird schema, whose root type is not an object.
        // Unlikely but allowed according to our rules.
        other
    }
    field.copy(fieldType = fieldType)
  }
}
