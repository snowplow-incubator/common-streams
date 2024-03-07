/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import cats.effect.testing.specs2.CatsEffect
import io.circe.literal.JsonStringContext
import io.circe.{Decoder, DecodingFailure}
import org.specs2.Specification

class LicenseSpec extends Specification with CatsEffect {

  implicit val decoder: Decoder[AcceptedLicense] =
    AcceptedLicense.decoder(AcceptedLicense.DocumentationLink("https://test.license.doc"))

  private val expectedErrorMessage =
    "DecodingFailure at : Please accept the terms of the Snowplow Limited Use License Agreement to proceed. See https://test.license.doc for more information on the license and how to configure this."

  def is = s2"""
      Decoding license should be successful for:
        boolean true $e1
        string "true" $e2
        string "yes" $e3
        string "on" $e4
        string "1" $e5
      Decoding license should not be successful for:
        boolean false $e6
        string "false" $e7
        any 'non-truthy' string like "something" $e8
        """

  def e1 =
    json"""{"accept": true}""".as[AcceptedLicense] must beRight(AcceptedLicense())

  def e2 =
    json"""{"accept": "true"}""".as[AcceptedLicense] must beRight(AcceptedLicense())

  def e3 =
    json"""{"accept": "yes"}""".as[AcceptedLicense] must beRight(AcceptedLicense())

  def e4 =
    json"""{"accept": "on"}""".as[AcceptedLicense] must beRight(AcceptedLicense())

  def e5 =
    json"""{"accept": "1"}""".as[AcceptedLicense] must beRight(AcceptedLicense())

  def e6 =
    json"""{"accept": false}""".as[AcceptedLicense] must beLeft.like { case er: DecodingFailure =>
      er.getMessage must beEqualTo(expectedErrorMessage)
    }

  def e7 =
    json"""{"accept": "false"}""".as[AcceptedLicense] must beLeft.like { case er: DecodingFailure =>
      er.getMessage must beEqualTo(expectedErrorMessage)
    }

  def e8 =
    json"""{"accept": "something"}""".as[AcceptedLicense] must beLeft.like { case er: DecodingFailure =>
      er.getMessage must beEqualTo(expectedErrorMessage)
    }
}
