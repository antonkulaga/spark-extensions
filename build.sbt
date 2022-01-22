import sbt.Keys._

import sbt._

name := "spark-extensions"

organization := "com.github.antonkulaga"

scalaVersion :=  "2.13.6"

version := "0.2.3"

isSnapshot := false

scalacOptions ++= Seq("-feature", "-language:_" )

javacOptions ++= Seq("-Xlint", "-J-Xss5M", "-encoding", "UTF-8")

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

Compile / mainClass := Some("comp.bio.aging.extractor.Main")

Test / resourceDirectory := baseDirectory { _ / "files" }.value

Compile / unmanagedClasspath ++= (Compile / unmanagedResources).value

resolvers += Resolver.mavenLocal

resolvers += Resolver.sonatypeRepo("releases")

resolvers += ("ICM repository" at "http://maven.icm.edu.pl/artifactory/repo").withAllowInsecureProtocol(true)

resolvers += "jitpack.io" at "https://jitpack.io"

lazy val sparkVersion = "3.2.0"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion,

  "org.apache.spark" %% "spark-mllib" % sparkVersion,

  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "org.typelevel" %% "cats-core" % "2.7.0",

  "org.scalatest" %% "scalatest" % "3.2.10" % Test,

  "com.lihaoyi" %% "ammonite-ops" % "2.4.1"

  //"com.holdenkarau" %% "spark-testing-base" % "2.4.4_0.12.0" % Test
)

resolvers += "jitpack" at "https://jitpack.io"

//initialCommands in (Test, console) := """ammonite.Main().run()"""

Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oF")

exportJars := true

run / fork := true

Test / parallelExecution := false

licenses += ("MPL-2.0", url("http://opensource.org/licenses/MPL-2.0"))
