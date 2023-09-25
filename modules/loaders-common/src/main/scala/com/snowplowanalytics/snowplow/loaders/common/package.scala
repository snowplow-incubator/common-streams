/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders

import cats.Order
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema

package object common {

  private[common] implicit val orderingSchemaKey: Ordering[SchemaKey] = SchemaKey.ordering

  private[common] implicit val orderSDS: Order[SelfDescribingSchema[Schema]] =
    Order.fromOrdering[SelfDescribingSchema[Schema]](
      Ordering.by(_.self.schemaKey)
    )

  /* Represents schema revision and addition for a given schema model */
  type SchemaSubVersion = (Int, Int)
}
