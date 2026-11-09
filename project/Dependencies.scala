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
    // Scala and FP core
    val cats             = "2.13.0"
    val catsEffect       = "3.7.1"
    val catsRetry        = "3.1.3"
    val fs2              = "3.14.0"
    val log4cats         = "2.7.1"
    val decline          = "2.4.1"
    val circe            = "0.14.16"
    val circeExtra       = "0.14.4"
    val circeConfig      = "0.10.2"
    val collectionCompat = "2.11.0"

    // HTTP
    val http4s = "0.23.37"
    val blaze  = "0.23.18"

    // Snowplow libraries
    val schemaDdl    = "0.28.0"
    val badrows      = "2.3.0"
    val igluClient   = "4.3.0"
    val igluCore     = "1.1.6"
    val analyticsSdk = "3.2.3"

    // Stream clients, and the cloud SDKs and auth they need
    val fs2Kafka       = "3.9.1"
    val pubsub         = "1.155.0"
    val awsSdk2        = "2.54.18"
    val kinesisClient  = "2.7.3"
    val kinesisClient3 = "3.4.1"
    val mskAuth        = "2.3.8"
    val azureSdk       = "1.18.6"
    val jwt            = "10.5"
    val nsq            = "1.6.0"

    // Compression
    val zstd = "1.5.7-16"

    // Logging, metrics and error reporting
    val slf4j      = "2.0.19"
    val micrometer = "1.15.12"
    val sentry     = "7.16.0"

    // Versions held above what something upstream asks for, to address security vulnerabilities.
    // See the *SecurityOverrides Seqs below for where each one applies.
    val jawn         = "1.7.0"
    val kafkaClients = "3.9.2"
    val lz4          = "1.11.4"
    val jackson      = "2.22.3"

    // Test only
    val specs2           = "4.20.0"
    val catsEffectSpecs2 = "1.7.0"
    val localstack       = "1.21.4"
    val eventGen         = "0.7.0"
    val dockerJava       = "3.7.0"

    // Compiler plugins
    val betterMonadicFor = "0.3.1"
    val kindProjector    = "0.13.4"
  }

  // Scala and FP core
  val catsEffectKernel  = "org.typelevel"          %% "cats-effect-kernel"      % V.catsEffect
  val cats              = "org.typelevel"          %% "cats-core"               % V.cats
  val fs2               = "co.fs2"                 %% "fs2-core"                % V.fs2
  val log4cats          = "org.typelevel"          %% "log4cats-slf4j"          % V.log4cats
  val catsRetry         = "com.github.cb372"       %% "cats-retry"              % V.catsRetry
  val decline           = "com.monovore"           %% "decline-effect"          % V.decline
  val circeConfig       = "io.circe"               %% "circe-config"            % V.circeConfig
  val circeGeneric      = "io.circe"               %% "circe-generic"           % V.circe
  val circeGenericExtra = "io.circe"               %% "circe-generic-extras"    % V.circeExtra
  val circeLiteral      = "io.circe"               %% "circe-literal"           % V.circe
  val collectionCompat  = "org.scala-lang.modules" %% "scala-collection-compat" % V.collectionCompat

  // HTTP
  val emberServer = "org.http4s" %% "http4s-ember-server" % V.http4s
  val http4sCirce = "org.http4s" %% "http4s-circe"        % V.http4s
  val http4sDsl   = "org.http4s" %% "http4s-dsl"          % V.http4s
  val blazeClient = "org.http4s" %% "http4s-blaze-client" % V.blaze
  val blazeServer = "org.http4s" %% "http4s-blaze-server" % V.blaze

  // Snowplow libraries
  val badrows      = "com.snowplowanalytics" %% "snowplow-badrows"             % V.badrows
  val schemaDdl    = "com.snowplowanalytics" %% "schema-ddl"                   % V.schemaDdl
  val igluClient   = "com.snowplowanalytics" %% "iglu-scala-client"            % V.igluClient
  val igluCore     = "com.snowplowanalytics" %% "iglu-core"                    % V.igluCore
  val analyticsSdk = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk

  // Stream clients, and the cloud SDKs and auth they need
  val fs2Kafka       = "com.github.fd4s"       %% "fs2-kafka"           % V.fs2Kafka
  val pubsub         = "com.google.cloud"       % "google-cloud-pubsub" % V.pubsub
  val arnsSdk2       = "software.amazon.awssdk" % "arns"                % V.awsSdk2
  val kinesisSdk2    = "software.amazon.awssdk" % "kinesis"             % V.awsSdk2
  val dynamoDbSdk2   = "software.amazon.awssdk" % "dynamodb"            % V.awsSdk2
  val cloudwatchSdk2 = "software.amazon.awssdk" % "cloudwatch"          % V.awsSdk2
  val kinesisClient = ("software.amazon.kinesis" % "amazon-kinesis-client" % V.kinesisClient)
    .exclude("com.amazonaws", "amazon-kinesis-producer")
    .exclude("software.amazon.glue", "schema-registry-build-tools")
    .exclude("software.amazon.glue", "schema-registry-common")
    .exclude("software.amazon.glue", "schema-registry-serde")
  val kinesisClient3 = ("software.amazon.kinesis" % "amazon-kinesis-client" % V.kinesisClient3)
    .exclude("com.amazonaws", "amazon-kinesis-producer")
    .exclude("software.amazon.glue", "schema-registry-build-tools")
    .exclude("software.amazon.glue", "schema-registry-common")
    .exclude("software.amazon.glue", "schema-registry-serde")
  val mskAuth       = "software.amazon.msk" % "aws-msk-iam-auth" % V.mskAuth % Runtime
  val azureIdentity = "com.azure"           % "azure-identity"   % V.azureSdk
  val jwt           = "com.nimbusds"        % "nimbus-jose-jwt"  % V.jwt
  val nsq           = "com.sproutsocial"    % "nsq-j"            % V.nsq

  // Compression
  val zstd = "com.github.luben" % "zstd-jni" % V.zstd

  // Logging, metrics and error reporting
  val slf4jApi             = "org.slf4j"     % "slf4j-api"                      % V.slf4j
  val slf4jSimple          = "org.slf4j"     % "slf4j-simple"                   % V.slf4j
  val micrometerCore       = "io.micrometer" % "micrometer-core"                % V.micrometer
  val micrometerPrometheus = "io.micrometer" % "micrometer-registry-prometheus" % V.micrometer
  val sentry               = "io.sentry"     % "sentry"                         % V.sentry

  // Artifacts named only to force a version up; grouped into the *SecurityOverrides Seqs below
  val jawnParser      = "org.typelevel"             %% "jawn-parser"      % V.jawn
  val kafkaClients    = "org.apache.kafka"           % "kafka-clients"    % V.kafkaClients
  val lz4             = "at.yawk.lz4"                % "lz4-java"         % V.lz4
  val jacksonDatabind = "com.fasterxml.jackson.core" % "jackson-databind" % V.jackson

  // Test only
  val specs2            = "org.specs2"            %% "specs2-core"                   % V.specs2           % Test
  val specs2Scalacheck  = "org.specs2"            %% "specs2-scalacheck"             % V.specs2           % Test
  val catsEffectTestkit = "org.typelevel"         %% "cats-effect-testkit"           % V.catsEffect       % Test
  val catsEffectSpecs2  = "org.typelevel"         %% "cats-effect-testing-specs2"    % V.catsEffectSpecs2 % Test
  val eventGen          = "com.snowplowanalytics" %% "snowplow-event-generator-core" % V.eventGen         % Test
  val dockerJava        = "com.github.docker-java" % "docker-java"                   % V.dockerJava       % Test
  val localstack        = "org.testcontainers"     % "localstack"                    % V.localstack       % Test

  // Compiler plugins
  val betterMonadicFor = "com.olegpy"    %% "better-monadic-for" % V.betterMonadicFor
  val kindProjector    = "org.typelevel" %% "kind-projector"     % V.kindProjector cross CrossVersion.full

  // Security overrides, grouped by the module they apply to. An entry is needed only while it still
  // evicts something: once the upstream catches up, delete it. To check, drop the line and compare
  // the resolved runtime classpath before and after; reading the dependency tree for eviction
  // markers is unreliable, because sbt truncates long lines.
  //
  // Naming jackson-databind pulls jackson-core up alongside it.

  // circe-parser, via circe-config and snowplow-badrows, brings an older jawn-parser. This also
  // covers the stream modules, which depend on streams-core.
  val streamsSecurityOverrides = Seq(jawnParser)

  // amazon-kinesis-client 3.x brings an older jackson-databind.
  val kinesis3SecurityOverrides = Seq(jacksonDatabind)

  // fs2-kafka trails kafka-clients' fixes, and kafka-clients in turn brings an older lz4-java.
  // jackson-databind comes in via azure-identity and aws-msk-iam-auth.
  val kafkaSecurityOverrides = Seq(kafkaClients, lz4, jacksonDatabind)

  // iglu-scala-client brings an older jackson-databind.
  val runtimeCommonSecurityOverrides = Seq(jacksonDatabind)

  // iglu-scala-client and schema-ddl bring an older jackson-databind.
  val loadersCommonSecurityOverrides = Seq(jacksonDatabind)

  val streamsDependencies = Seq(
    cats,
    catsEffectKernel,
    fs2,
    log4cats,
    catsRetry,
    collectionCompat,
    blazeClient,
    blazeServer,
    http4sDsl,
    circeConfig,
    circeGeneric,
    circeGenericExtra,
    specs2,
    specs2Scalacheck,
    catsEffectSpecs2,
    catsEffectTestkit,
    slf4jApi,
    slf4jSimple % Test,
    zstd,
    badrows
  ) ++ streamsSecurityOverrides

  val kinesisDependencies = Seq(
    kinesisClient,
    arnsSdk2,
    kinesisSdk2,
    dynamoDbSdk2,
    cloudwatchSdk2,
    circeConfig,
    circeGeneric,
    circeGenericExtra,
    circeLiteral % Test,
    specs2
  )

  val kinesis3Dependencies = Seq(
    kinesisClient3,
    arnsSdk2,
    kinesisSdk2,
    dynamoDbSdk2,
    cloudwatchSdk2,
    circeConfig,
    circeGeneric,
    circeGenericExtra,
    circeLiteral % Test,
    specs2
  ) ++ kinesis3SecurityOverrides

  val kinesisItDependencies = Seq(
    circeGeneric % Test,
    catsEffectTestkit,
    catsEffectSpecs2,
    catsRetry % Test,
    localstack,
    slf4jSimple % Test,
    dockerJava
  )

  val kafkaDependencies = Seq(
    fs2Kafka,
    mskAuth,
    circeConfig,
    circeGeneric,
    azureIdentity,
    jwt,
    specs2
  ) ++ kafkaSecurityOverrides

  val pubsubDependencies = Seq(
    pubsub,
    circeConfig,
    circeGeneric,
    collectionCompat,
    specs2
  )

  val nsqDependencies = Seq(
    nsq,
    circeConfig,
    circeGeneric,
    specs2
  )

  val runtimeCommonDependencies = Seq(
    blazeClient,
    cats,
    catsEffectKernel,
    catsRetry,
    circeConfig,
    circeGeneric,
    emberServer,
    fs2,
    http4sCirce,
    http4sDsl,
    igluClient,
    igluCore,
    log4cats,
    micrometerCore,
    micrometerPrometheus,
    slf4jApi,
    sentry,
    specs2,
    catsEffectSpecs2,
    catsEffectTestkit,
    circeLiteral % Test
  ) ++ runtimeCommonSecurityOverrides

  val loadersCommonDependencies = Seq(
    cats,
    catsEffectKernel,
    schemaDdl,
    badrows,
    circeLiteral % Test,
    igluClient,
    igluCore,
    analyticsSdk,
    specs2,
    specs2Scalacheck,
    catsEffectSpecs2,
    slf4jSimple % Test,
    eventGen
  ) ++ loadersCommonSecurityOverrides
}
