/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */

// SBT
import sbt.Keys.*
import sbt.*

// scalafmt
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._

// dynver plugin
import sbtdynver.DynVerPlugin.autoImport._

// Mima plugin
import com.typesafe.tools.mima.plugin.MimaKeys._

// Site plugin
import com.typesafe.sbt.site.SitePreviewPlugin.autoImport.previewPath
import com.typesafe.sbt.site.SitePlugin.autoImport._
import com.typesafe.sbt.site.SiteScaladocPlugin.autoImport._

// ghpages
import com.github.sbt.git.SbtGit.git
import com.github.sbt.sbtghpages.GhpagesPlugin.autoImport.ghpagesNoJekyll

object BuildSettings {

  lazy val scala212 = "2.12.18"
  lazy val scala213 = "2.13.12"

  lazy val buildSettings = Seq(
    organization := "com.snowplowanalytics",
    scalaVersion := scala213,
    crossScalaVersions := List(scala212, scala213),
    scalafmtConfig := file(".scalafmt.conf"),
    scalafmtOnCompile := false,
    scalacOptions += "-Ywarn-macros:after",
    Test / fork := true,
    addCompilerPlugin(Dependencies.betterMonadicFor),
    ThisBuild / autoAPIMappings := true,
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    ThisBuild / dynverSeparator := "-", // to be compatible with docker

    Compile / resourceGenerators += Def.task {
      val license = (Compile / resourceManaged).value / "META-INF" / "LICENSE"
      IO.copyFile(file("LICENSE.md"), license)
      Seq(license)
    }.taskValue
  )

  lazy val publishSettings = Seq[Setting[_]](
    publishArtifact := true,
    Test / publishArtifact := false,
    pomIncludeRepository := { _ => false },
    homepage := Some(url("https://snowplow.io")),
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    ThisBuild / licenses := Seq("SCL" -> file("LICENSE.md").toURI.toURL),
    developers := List(
      Developer(
        "Snowplow Analytics Ltd",
        "Snowplow Analytics Ltd",
        "support@snowplowanalytics.com",
        url("https://snowplow.io")
      )
    )
  )

  // If new version introduces breaking changes,
  // clear-out mimaBinaryIssueFilters and mimaPreviousVersions.
  // Otherwise, add previous version to set without
  // removing other versions.
  val mimaPreviousVersions: Set[String] = Set()

  lazy val mimaSettings = Seq(
    mimaPreviousArtifacts := {
      mimaPreviousVersions.map {
        organization.value %% name.value % _
      }
    },
    ThisBuild / mimaFailOnNoPrevious := false,
    mimaBinaryIssueFilters ++= Seq(),
    Test / test := {
      mimaReportBinaryIssues.value
      (Test / test).value
    }
  )

  val docsSettings = Seq(
    SiteScaladoc / siteSubdirName := s"${moduleName.value}/${version.value}",
    previewPath := s"${moduleName.value}/${version.value}"
  )

  val ghPagesSettings = Seq(
    git.remoteRepo := "git@github.com:snowplow-incubator/common-streams.git",
    ghpagesNoJekyll := true
  )
}
