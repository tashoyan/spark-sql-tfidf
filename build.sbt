
name := "spark-sql-tfidf"
version := "0.1-SNAPSHOT"
scalaVersion := "2.11.12"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-target:jvm-1.8",
  "-unchecked",
  "-Ywarn-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused",
  "-Ywarn-value-discard",
  "-Xfuture",
  "-Xlint"
)

javacOptions ++= Seq(
  "-Xlint:deprecation",
  "-Xlint:unchecked",
  "-source", "1.8",
  "-target", "1.8",
  "-g:vars"
)

val sparkVersion = "2.2.1"

libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

fork := true

import sbtassembly.MergeStrategy
assemblyMergeStrategy in assembly := { // this is the default plus one more for mime.types
  // See https://github.com/sbt/sbt-assembly#merge-strategy
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat

  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename

  case PathList(ps @ _*) if ps.last.startsWith("CHANGELOG.") =>
    MergeStrategy.discard

  case PathList("META-INF", xs @ _*) =>
    xs map {_.toLowerCase} match {
      case "mime.types" :: _ =>
        MergeStrategy.filterDistinctLines

      case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil =>
        MergeStrategy.discard

      case ps @ x :: _ if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard

      case "plexus" :: _ =>
        MergeStrategy.discard

      case "services" :: _ =>
        MergeStrategy.filterDistinctLines

      case "spring.schemas" :: Nil | "spring.handlers" :: Nil =>
        MergeStrategy.filterDistinctLines

      case _ => MergeStrategy.deduplicate
    }

  case _ => MergeStrategy.deduplicate
}
