import com.typesafe.sbt.site.util.SiteHelpers

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
    runtimeCommon,
    loadersCommon
  )

lazy val streams: Project = project
  .settings(
    name := "streams-core"
  )
  .withId("streams-core")
  .in(file("modules/streams-core"))
  .enablePlugins(SiteScaladocPlugin, PreprocessPlugin, SitePreviewPlugin)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.mimaSettings)
  .settings(BuildSettings.docsSettings)
  .settings(
    previewFixedPort := Some(9995),
    Preprocess / preprocessVars := Map("VERSION" -> version.value)
  )
  .settings(libraryDependencies ++= Dependencies.streamsDependencies)

lazy val kinesis: Project = project
  .in(file("modules/kinesis"))
  .enablePlugins(SiteScaladocPlugin, PreprocessPlugin, SitePreviewPlugin)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.mimaSettings)
  .settings(BuildSettings.docsSettings)
  .settings(
    previewFixedPort := Some(9996),
    Preprocess / preprocessVars := Map("VERSION" -> version.value)
  )
  .settings(libraryDependencies ++= Dependencies.kinesisDependencies)
  .dependsOn(streams)

lazy val kinesisIT: Project = project
  .settings(
    name := "kinesis-it"
  )
  .withId("kinesis-it")
  .in(file("modules/kinesis-it"))
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.mimaSettings)
  .dependsOn(kinesis)
  .settings(
    publish / skip := true,
    publishLocal / skip := true,
    Test / javaOptions ++= Seq("-Daws.region=eu-central-1", "-Daws.accessKeyId=test", "-Daws.secretAccessKey=test"),
    libraryDependencies ++= Dependencies.kinesisItDependencies
  )

lazy val kafka: Project = project
  .in(file("modules/kafka"))
  .enablePlugins(SiteScaladocPlugin, PreprocessPlugin, SitePreviewPlugin)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.mimaSettings)
  .settings(BuildSettings.docsSettings)
  .settings(
    previewFixedPort := Some(9997),
    Preprocess / preprocessVars := Map("VERSION" -> version.value)
  )
  .settings(libraryDependencies ++= Dependencies.kafkaDependencies)
  .dependsOn(streams)

lazy val pubsub: Project = project
  .in(file("modules/pubsub"))
  .enablePlugins(SiteScaladocPlugin, PreprocessPlugin, SitePreviewPlugin)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.mimaSettings)
  .settings(BuildSettings.docsSettings)
  .settings(
    previewFixedPort := Some(9998),
    Preprocess / preprocessVars := Map("VERSION" -> version.value)
  )
  .settings(libraryDependencies ++= Dependencies.pubsubDependencies)
  .dependsOn(streams)

lazy val runtimeCommon: Project = project
  .settings(
    name := "runtime-common"
  )
  .withId("runtime-common")
  .in(file("modules/runtime-common"))
  .enablePlugins(SiteScaladocPlugin, PreprocessPlugin, SitePreviewPlugin)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.mimaSettings)
  .settings(BuildSettings.docsSettings)
  .settings(
    previewFixedPort := Some(9999),
    Preprocess / preprocessVars := Map("VERSION" -> version.value)
  )
  .settings(libraryDependencies ++= Dependencies.runtimeCommonDependencies)

lazy val loadersCommon: Project = project
  .settings(
    name := "loaders-common"
  )
  .withId("loaders-common")
  .in(file("modules/loaders-common"))
  .enablePlugins(SiteScaladocPlugin, PreprocessPlugin, SitePreviewPlugin)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.mimaSettings)
  .settings(BuildSettings.docsSettings)
  .settings(
    previewFixedPort := Some(9999),
    Preprocess / preprocessVars := Map("VERSION" -> version.value)
  )
  .settings(libraryDependencies ++= Dependencies.loadersCommonDependencies)

// docs

val StreamsCore   = config("streams-core")
val Kinesis       = config("kinesis")
val Kafka         = config("kafka")
val PubSub        = config("pubsub")
val RuntimeCommon = config("runtime-common")
val LoadersCommon = config("loaders-common")

lazy val scaladocSiteProjects: List[(Project, sbt.Configuration)] = List(
  (streams, StreamsCore),
  (kinesis, Kinesis),
  (kafka, Kafka),
  (pubsub, PubSub),
  (runtimeCommon, RuntimeCommon),
  (loadersCommon, LoadersCommon)
)

lazy val scaladocSiteSettings = scaladocSiteProjects.flatMap { case (project, conf) =>
  inConfig(conf)(
    Seq(
      siteSubdirName := s"${project.id}/${version.value}",
      mappings := (project / Compile / packageDoc / mappings).value
    )
  ) ++
    SiteHelpers.addMappingsToSiteDir(conf / mappings, conf / siteSubdirName)
}

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(SiteScaladocPlugin, PreprocessPlugin, SitePreviewPlugin, GhpagesPlugin)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.ghPagesSettings)
  .settings(
    publish / skip := true,
    publishLocal / skip := true,
    Preprocess / preprocessVars := Map("VERSION" -> version.value)
  )
  .settings(scaladocSiteSettings)
