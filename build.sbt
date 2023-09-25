/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */

lazy val root = project
  .in(file("."))
  .aggregate(
    streams,
    kinesis,
    kinesisIT,
    kafka,
    pubsub,
    loadersCommon
  )

lazy val streams: Project = project
  .in(file("modules/streams-core"))
  .enablePlugins(SiteScaladocPlugin, PreprocessPlugin, SitePreviewPlugin, GhpagesPlugin)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.mimaSettings)
  .settings(BuildSettings.docsSettings)
  .settings(BuildSettings.ghPagesSettings)
  .settings(
    previewFixedPort := Some(9995),
    Preprocess / preprocessVars := Map("VERSION" -> version.value)
  )
  .settings(libraryDependencies ++= Dependencies.streamsDependencies)

lazy val kinesis: Project = project
  .in(file("modules/kinesis"))
  .enablePlugins(SiteScaladocPlugin, PreprocessPlugin, SitePreviewPlugin, GhpagesPlugin)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.mimaSettings)
  .settings(BuildSettings.docsSettings)
  .settings(BuildSettings.ghPagesSettings)
  .settings(
    previewFixedPort := Some(9996),
    Preprocess / preprocessVars := Map("VERSION" -> version.value)
  )
  .settings(libraryDependencies ++= Dependencies.kinesisDependencies)
  .dependsOn(streams)

lazy val kinesisIT: Project = project
  .in(file("modules/kinesis-it"))
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.mimaSettings)
  .dependsOn(kinesis)
  .settings(
    publish / skip := true,
    publishLocal / skip := true,
    /**
     * AWS_REGION=eu-central-1 is detected by the lib & integration test suite which follows the
     * same region resolution mechanism as the lib
     */
    ThisProject / envVars := Map("AWS_REGION" -> "eu-central-1"),
    libraryDependencies ++= Dependencies.kinesisItDependencies
  )

lazy val kafka: Project = project
  .in(file("modules/kafka"))
  .enablePlugins(SiteScaladocPlugin, PreprocessPlugin, SitePreviewPlugin, GhpagesPlugin)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.mimaSettings)
  .settings(BuildSettings.docsSettings)
  .settings(BuildSettings.ghPagesSettings)
  .settings(
    previewFixedPort := Some(9997),
    Preprocess / preprocessVars := Map("VERSION" -> version.value)
  )
  .settings(libraryDependencies ++= Dependencies.kafkaDependencies)
  .dependsOn(streams)

lazy val pubsub: Project = project
  .in(file("modules/pubsub"))
  .enablePlugins(SiteScaladocPlugin, PreprocessPlugin, SitePreviewPlugin, GhpagesPlugin)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.mimaSettings)
  .settings(BuildSettings.docsSettings)
  .settings(BuildSettings.ghPagesSettings)
  .settings(
    previewFixedPort := Some(9998),
    Preprocess / preprocessVars := Map("VERSION" -> version.value)
  )
  .settings(libraryDependencies ++= Dependencies.pubsubDependencies)
  .dependsOn(streams)

lazy val loadersCommon: Project = project
  .in(file("modules/loaders-common"))
  .enablePlugins(SiteScaladocPlugin, PreprocessPlugin, SitePreviewPlugin, GhpagesPlugin)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.mimaSettings)
  .settings(BuildSettings.docsSettings)
  .settings(BuildSettings.ghPagesSettings)
  .settings(
    previewFixedPort := Some(9999),
    Preprocess / preprocessVars := Map("VERSION" -> version.value)
  )
  .settings(libraryDependencies ++= Dependencies.loadersCommonDependencies)
