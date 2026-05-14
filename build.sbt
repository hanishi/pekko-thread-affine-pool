ThisBuild / scalaVersion := "3.7.4"
ThisBuild / organization := "com.example"
ThisBuild / version      := "0.1.0"

val pekkoVersion     = "1.6.0"
val pekkoHttpVersion = "1.3.0"
val graalVersion     = "25.0.3"

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "pekko-thread-affine-pool",
    Compile / mainClass := Some("example.Main"),
    libraryDependencies ++= Seq(
      "org.apache.pekko"     %% "pekko-actor-typed"          % pekkoVersion,
      "org.apache.pekko"     %% "pekko-stream"               % pekkoVersion,
      "org.apache.pekko"     %% "pekko-slf4j"                % pekkoVersion,
      "org.apache.pekko"     %% "pekko-http"                 % pekkoHttpVersion,
      "ch.qos.logback"        % "logback-classic"            % "1.5.32",
      "org.duckdb"            % "duckdb_jdbc"                % "1.5.3.0",
      "com.microsoft.playwright" % "playwright"              % "1.60.0",
      "org.graalvm.polyglot"  % "polyglot"                   % graalVersion,
      "org.graalvm.polyglot"  % "js-community"               % graalVersion % Runtime pomOnly(),
      "org.apache.pekko"     %% "pekko-actor-testkit-typed"  % pekkoVersion % Test,
      "org.scalatest"        %% "scalatest"                  % "3.2.20"     % Test,
    ),
    Test / fork := true,
    Test / javaOptions += "-Dpolyglot.engine.WarnInterpreterOnly=false",
  )
