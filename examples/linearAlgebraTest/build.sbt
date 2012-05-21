name := "Linear Algebra Test"

version := "0.1"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-deprecation", "-Ydependent-method-types")

libraryDependencies ++= Seq(
    "com.nicta" %% "scoobi" % "0.4.0-SNAPSHOT" % "provided",
    "org.apache.commons" % "commons-math" % "2.2")
