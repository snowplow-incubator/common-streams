/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.runtime

import java.nio.file.Paths
import io.circe.literal._
import io.circe.Decoder
import io.circe.generic.semiauto._
import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import org.specs2.Specification
import com.snowplowanalytics.iglu.client.resolver.Resolver

class ConfigParserSpec extends Specification with CatsEffect {
  import ConfigParserSpec._

  def is = s2"""
  ConfigParser should
    parse iglu resolver json correctly $parseIgluResolverCorrectly
    fail when iglu resolver json is missing required field $igluResolverMissingField
    fail when iglu resolver json is invalid $igluResolverInvalid
    fail when iglu resolver json file doesn't exist $igluResolverDoesNotExist
    parse config with custom class correctly $parseCustomClassConfig
    parse config with 'snowplow' namespace correctly $parseConfigWithCorrectNamespace
    parse config with substitution correctly $parseConfigWithSubstitution
    fail when wrong namespace is used in the config $configWithWrongNamespace
    fail when config hocon is missing required field $configMissingField
    fail when config hocon is invalid $configInvalid
    fail when config hocon file doesn't exist $configDoesNotExist
    parse config with env variable correctly $parseConfigWithEnvVariable
    fail when env variable in the config isn't set $failWhenEnvVariableNotSet
  """

  def parseIgluResolverCorrectly = {
    val expected = Resolver.ResolverConfig(
      cacheSize = 500,
      cacheTtl  = None,
      repositoryRefs = List(
        json"""
          {
            "name": "Iglu Central",
            "priority": 0,
            "vendorPrefixes": [ "com.snowplowanalytics" ],
            "connection": {
              "http": {
                "uri": "http://iglucentral.com"
              }
            }
          }
        """,
        json"""
          {
            "name": "Iglu Central - GCP Mirror",
            "priority": 1,
            "vendorPrefixes": [ "com.snowplowanalytics" ],
            "connection": {
              "http": {
                "uri": "http://mirror01.iglucentral.com"
              }
            }
          }
        """
      )
    )
    val path = Paths.get("src/test/resources/config_parser_test/iglu_resolver.json")
    ConfigParser
      .igluResolverFromFile[IO](path)
      .value
      .map(_ must beRight(expected))
  }

  def igluResolverMissingField = {
    val expected = "DecodingFailure at .cacheSize: Missing required field"
    val path     = Paths.get("src/test/resources/config_parser_test/iglu_resolver_missing_field.json")
    ConfigParser
      .igluResolverFromFile[IO](path)
      .value
      .map(_ must beLeft(expected))
  }

  def igluResolverInvalid = {
    val expected =
      "String: 6: List should have ended with ] or had a comma, instead had token: ':' (if you want ':' to be part of a string value, then double-quote it)"
    val path = Paths.get("src/test/resources/config_parser_test/iglu_resolver_invalid.json")
    ConfigParser
      .igluResolverFromFile[IO](path)
      .value
      .map(_ must beLeft(expected))
  }

  def igluResolverDoesNotExist = {
    val expectedPattern = "Error reading .*/iglu_resolver_nonexist.json file from filesystem: .*/iglu_resolver_nonexist.json".r
    val path            = Paths.get("src/test/resources/config_parser_test/iglu_resolver_nonexist.json")
    ConfigParser
      .igluResolverFromFile[IO](path)
      .value
      .map(_ must beLike { case Left(expectedPattern(_*)) => ok })
  }

  def parseCustomClassConfig = {
    val expected = TestConfig(
      field1 = "value1",
      field2 = 10,
      field3 = true,
      field4 = TestConfig.Subfield(field41 = "value41", field42 = "value42")
    )
    val path = Paths.get("src/test/resources/config_parser_test/config.hocon")
    ConfigParser
      .configFromFile[IO, TestConfig](path)
      .value
      .map(_ must beRight(expected))
  }

  def parseConfigWithCorrectNamespace = {
    val expected = TestConfig(
      field1 = "value1",
      field2 = 10,
      field3 = true,
      field4 = TestConfig.Subfield(field41 = "value41", field42 = "value42")
    )
    val path = Paths.get("src/test/resources/config_parser_test/config_correct_namespace.hocon")
    ConfigParser
      .configFromFile[IO, TestConfig](path)
      .value
      .map(_ must beRight(expected))
  }

  def parseConfigWithSubstitution = {
    val expected = TestConfig(
      field1 = "sub1",
      field2 = 10,
      field3 = true,
      field4 = TestConfig.Subfield(field41 = "sub2", field42 = "value42")
    )
    val path = Paths.get("src/test/resources/config_parser_test/config_with_substitution.hocon")
    ConfigParser
      .configFromFile[IO, TestConfig](path)
      .value
      .map(_ must beRight(expected))
  }

  def configWithWrongNamespace = {
    val expected = "Cannot resolve config: DecodingFailure at .field1: Missing required field"
    val path     = Paths.get("src/test/resources/config_parser_test/config_wrong_namespace.hocon")
    ConfigParser
      .configFromFile[IO, TestConfig](path)
      .value
      .map(_ must beLeft(expected))
  }

  def configMissingField = {
    val expected = "Cannot resolve config: DecodingFailure at .field41: Missing required field"
    val path     = Paths.get("src/test/resources/config_parser_test/config_missing_field.hocon")
    ConfigParser
      .configFromFile[IO, TestConfig](path)
      .value
      .map(_ must beLeft(expected))
  }

  def configInvalid = {
    val expected =
      "String: 6: Expecting close brace } or a comma, got ':' (if you intended ':' to be part of a key or string value, try enclosing the key or value in double quotes)"
    val path = Paths.get("src/test/resources/config_parser_test/config_invalid.hocon")
    ConfigParser
      .configFromFile[IO, TestConfig](path)
      .value
      .map(_ must beLeft(expected))
  }

  def configDoesNotExist = {
    val expectedPattern = """Error reading .*/config_nonexist.hocon file from filesystem: .*/config_nonexist.hocon""".r
    val path            = Paths.get("src/test/resources/config_parser_test/config_nonexist.hocon")
    ConfigParser
      .configFromFile[IO, TestConfig](path)
      .value
      .map(_ must beLike { case Left(expectedPattern(_*)) => ok })
  }

  def parseConfigWithEnvVariable = {
    val expected = TestConfig(
      field1 = "value1",
      field2 = 10,
      field3 = true,
      field4 = TestConfig.Subfield(field41 = "envValue", field42 = "value42")
    )
    val path = Paths.get("src/test/resources/config_parser_test/config_with_set_env.hocon")
    ConfigParser
      .configFromFile[IO, TestConfig](path)
      .value
      .map(_ must beRight(expected))
  }

  def failWhenEnvVariableNotSet = {
    val expected = "Cannot resolve config: String: 6: Could not resolve substitution to a value: ${UNSET_ENV_VAR}"
    val path     = Paths.get("src/test/resources/config_parser_test/config_with_unset_env.hocon")
    ConfigParser
      .configFromFile[IO, TestConfig](path)
      .value
      .map(_ must beLeft(expected))
  }
}

object ConfigParserSpec {

  case class TestConfig(
    field1: String,
    field2: Int,
    field3: Boolean,
    field4: TestConfig.Subfield
  )
  object TestConfig {
    case class Subfield(field41: String, field42: String)

    implicit val testConfigDecoder: Decoder[TestConfig] = deriveDecoder[TestConfig]
    implicit val subfieldDecoder: Decoder[Subfield]     = deriveDecoder[Subfield]
  }
}
