name := "spark-misc"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.8"

ensimeScalaVersion in ThisBuild := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.1" % "provided"

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.4.0"

libraryDependencies += "org.tpolecat" %% "doobie-core" % "0.5.3"

libraryDependencies += "org.tpolecat" %% "doobie-postgres" % "0.5.3"
