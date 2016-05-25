name := "spark-misc"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.4"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.0")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.1" % "provided"

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "1.1.5"

libraryDependencies += "org.tpolecat" %% "doobie-core" % "0.2.3"

libraryDependencies += "org.tpolecat" %% "doobie-contrib-postgresql" % "0.2.3"

