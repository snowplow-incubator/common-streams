/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
import sbt._

object Dependencies {

  object V {
    // Scala
    val cats             = "2.9.0"
    val catsEffect       = "3.5.2"
    val fs2              = "3.9.3"
    val log4cats         = "2.6.0"
    val http4s           = "0.23.15"
    val decline          = "2.4.1"
    val circe            = "0.14.6"
    val circeExtra       = "0.14.3"
    val circeConfig      = "0.10.0"
    val betterMonadicFor = "0.3.1"
    val collectionCompat = "2.11.0"
    val refined          = "0.11.0"

    // Streams
    val fs2Kafka      = "3.0.1"
    val pubsub        = "1.123.17"
    val fs2AwsKinesis = "4.1.0"
    val awsSdk2       = "2.20.135"
    val kinesisClient = "2.5.2"

    // java
    val slf4j    = "2.0.9"
    val azureSdk = "1.9.1"

    // Snowplow
    val schemaDdl    = "0.21.0"
    val badrows      = "2.2.1"
    val igluClient   = "3.1.0"
    val tracker      = "2.0.0"
    val analyticsSdk = "3.1.0"

    // Transitive overrides
    val snappy = "1.1.10.2"

    // tests
    val specs2           = "4.20.0"
    val catsEffectSpecs2 = "1.5.0"
    val localstack       = "1.19.0"
  }

  val catsEffectKernel  = "org.typelevel"          %% "cats-effect-kernel"      % V.catsEffect
  val cats              = "org.typelevel"          %% "cats-core"               % V.cats
  val fs2               = "co.fs2"                 %% "fs2-core"                % V.fs2
  val log4cats          = "org.typelevel"          %% "log4cats-slf4j"          % V.log4cats
  val emberServer       = "org.http4s"             %% "http4s-ember-server"     % V.http4s
  val decline           = "com.monovore"           %% "decline-effect"          % V.decline
  val circeConfig       = "io.circe"               %% "circe-config"            % V.circeConfig
  val circeGeneric      = "io.circe"               %% "circe-generic"           % V.circe
  val circeGenericExtra = "io.circe"               %% "circe-generic-extras"    % V.circeExtra
  val circeLiteral      = "io.circe"               %% "circe-literal"           % V.circe
  val betterMonadicFor  = "com.olegpy"             %% "better-monadic-for"      % V.betterMonadicFor
  val collectionCompat  = "org.scala-lang.modules" %% "scala-collection-compat" % V.collectionCompat
  val refined           = "eu.timepit"             %% "refined"                 % V.refined

  // streams
  val fs2Kafka = "com.github.fd4s" %% "fs2-kafka"           % V.fs2Kafka
  val pubsub   = "com.google.cloud" % "google-cloud-pubsub" % V.pubsub
  val fs2AwsKinesis = ("io.laserdisc" %% "fs2-aws-kinesis" % V.fs2AwsKinesis)
    .exclude("software.amazon.kinesis", "amazon-kinesis-client")
    .exclude("com.amazonaws", "amazon-kinesis-producer")
  val arnsSdk2       = "software.amazon.awssdk" % "arns"       % V.awsSdk2
  val kinesisSdk2    = "software.amazon.awssdk" % "kinesis"    % V.awsSdk2
  val dynamoDbSdk2   = "software.amazon.awssdk" % "dynamodb"   % V.awsSdk2
  val cloudwatchSdk2 = "software.amazon.awssdk" % "cloudwatch" % V.awsSdk2
  val kinesisClient = ("software.amazon.kinesis" % "amazon-kinesis-client" % V.kinesisClient)
    .exclude("com.amazonaws", "amazon-kinesis-producer")
    .exclude("software.amazon.glue", "schema-registry-build-tools")
    .exclude("software.amazon.glue", "schema-registry-common")
    .exclude("software.amazon.glue", "schema-registry-serde")
  val catsEffectTestingIt = "org.typelevel"     %% "cats-effect-testkit"        % V.catsEffect
  val catsEffectSpecs2It  = "org.typelevel"     %% "cats-effect-testing-specs2" % V.catsEffectSpecs2
  val localstackIt        = "org.testcontainers" % "localstack"                 % V.localstack
  val slf4jIt             = "org.slf4j"          % "slf4j-simple"               % V.slf4j

  // java
  val slf4jSimple   = "org.slf4j" % "slf4j-simple"   % V.slf4j
  val slf4jApi      = "org.slf4j" % "slf4j-api"      % V.slf4j
  val azureIdentity = "com.azure" % "azure-identity" % V.azureSdk

  // transitive overrides
  val snappy = "org.xerial.snappy" % "snappy-java" % V.snappy

  val badrows      = "com.snowplowanalytics" %% "snowplow-badrows"                      % V.badrows
  val tracker      = "com.snowplowanalytics" %% "snowplow-scala-tracker-core"           % V.tracker
  val trackerEmit  = "com.snowplowanalytics" %% "snowplow-scala-tracker-emitter-http4s" % V.tracker
  val schemaDdl    = "com.snowplowanalytics" %% "schema-ddl"                            % V.schemaDdl
  val igluClient   = "com.snowplowanalytics" %% "iglu-scala-client"                     % V.igluClient
  val analyticsSdk = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk"          % V.analyticsSdk

  // tests
  val specs2            = "org.specs2"    %% "specs2-core"                % V.specs2           % Test
  val catsEffectTestkit = "org.typelevel" %% "cats-effect-testkit"        % V.catsEffect       % Test
  val catsEffectSpecs2  = "org.typelevel" %% "cats-effect-testing-specs2" % V.catsEffectSpecs2 % Test

  val streamsDependencies = Seq(
    cats,
    catsEffectKernel,
    fs2,
    log4cats,
    specs2,
    catsEffectSpecs2,
    catsEffectTestkit,
    slf4jApi,
    slf4jSimple % Test
  )

  val kinesisDependencies = Seq(
    kinesisClient,
    fs2AwsKinesis,
    arnsSdk2,
    kinesisSdk2,
    dynamoDbSdk2,
    cloudwatchSdk2,
    refined,
    circeConfig,
    circeGeneric,
    circeGenericExtra,
    circeLiteral % Test,
    specs2
  )

  val kinesisItDependencies = Seq(
    catsEffectTestingIt % Test,
    catsEffectSpecs2It  % Test,
    localstackIt        % Test,
    slf4jIt             % Test
  )

  val kafkaDependencies = Seq(
    fs2Kafka,
    circeConfig,
    circeGeneric,
    snappy
  )

  val pubsubDependencies = Seq(
    pubsub,
    circeConfig,
    circeGeneric,
    collectionCompat
  )

  val runtimeCommonDependencies = Seq(
    cats,
    catsEffectKernel,
    circeConfig,
    circeGeneric,
    emberServer,
    fs2,
    igluClient,
    log4cats,
    slf4jApi,
    tracker,
    trackerEmit,
    specs2,
    catsEffectSpecs2,
    catsEffectTestkit,
    circeLiteral % Test
  )

  val loadersCommonDependencies = Seq(
    cats,
    catsEffectKernel,
    schemaDdl,
    badrows,
    circeLiteral % Test,
    igluClient,
    analyticsSdk,
    specs2,
    catsEffectSpecs2,
    slf4jSimple % Test
  )
}
