import sbt.Keys._

import sbt._

name := "spark-extensions"

organization := "group.research.aging"

scalaVersion :=  "2.11.12"

version := "0.0.7.1"

coursierMaxIterations := 200

isSnapshot := false

scalacOptions ++= Seq("-feature", "-language:_" )

javacOptions ++= Seq("-Xlint", "-J-Xss5M", "-encoding", "UTF-8")

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

mainClass in Compile := Some("comp.bio.aging.extractor.Main")

resourceDirectory in Test := baseDirectory { _ / "files" }.value

unmanagedClasspath in Compile ++= (unmanagedResources in Compile).value

resolvers += Resolver.mavenLocal

resolvers += Resolver.sonatypeRepo("releases")

resolvers += sbt.Resolver.bintrayRepo("comp-bio-aging", "main")

resolvers += "ICM repository" at "http://maven.icm.edu.pl/artifactory/repo"

resolvers += "jitpack.io" at "https://jitpack.io"

lazy val sparkVersion = "2.4.3"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion,

  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "org.apache.spark" %% "spark-mllib" % sparkVersion,

  "org.typelevel" %% "cats-core" % "1.6.0",
  
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,

  "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % Test
)

initialCommands in (Test, console) := """ammonite.Main().run()"""

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF")

exportJars := true

fork in run := true

parallelExecution in Test := false

bintrayRepository := "main"

bintrayOrganization := Some("comp-bio-aging")

licenses += ("MPL-2.0", url("http://opensource.org/licenses/MPL-2.0"))

libraryDependencies += "com.lihaoyi" % "ammonite" % "1.6.7" cross CrossVersion.full

sourceGenerators in Test += Def.task {
  val file = (sourceManaged in Test).value / "amm.scala"
  IO.write(file, """object amm extends App { ammonite.Main.main(args) }""")
  Seq(file)
}.taskValue
