/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import io.circe.Decoder

final case class AcceptedLicense()

object AcceptedLicense {
  final case class DocumentationLink(value: String)

  def decoder(documentationLink: DocumentationLink): Decoder[AcceptedLicense] = {
    val truthy = Set("true", "yes", "on", "1")
    Decoder
      .forProduct1("accept")((s: String) => truthy(s.toLowerCase()))
      .or(Decoder.forProduct1("accept")((b: Boolean) => b))
      .emap {
        case false =>
          Left(
            s"Please accept the terms of the Snowplow Limited Use License Agreement to proceed. See ${documentationLink.value} for more information on the license and how to configure this."
          )
        case _ =>
          Right(AcceptedLicense())
      }
  }
}
