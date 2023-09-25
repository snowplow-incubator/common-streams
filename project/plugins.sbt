addSbtPlugin("org.typelevel"         % "sbt-tpolecat"         % "0.5.0")
addSbtPlugin("org.scalameta"         % "sbt-scalafmt"         % "2.5.2")
addSbtPlugin("com.snowplowanalytics" % "sbt-snowplow-release" % "0.3.1")
addSbtPlugin("com.eed3si9n"          % "sbt-buildinfo"        % "0.11.0")
addSbtPlugin("com.github.sbt"        % "sbt-ci-release"       % "1.5.12")
addSbtPlugin("com.typesafe"          % "sbt-mima-plugin"      % "1.1.3")
addSbtPlugin("com.github.sbt"        % "sbt-site"             % "1.5.0")
addSbtPlugin(
  ("com.github.sbt" % "sbt-ghpages" % "0.8.0")
    .exclude("org.scala-lang.modules", "scala-xml_2.12")
)

libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
