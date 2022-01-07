name := "spark-excell-config"
version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "3.0.0" % "provided"

// === Integration === //
libraryDependencies += "com.crealytics" %% "spark-excel" % "3.2.0_0.16.0"
libraryDependencies += "io.delta"       %% "delta-core"  % "1.1.0"

// === Config  === //
libraryDependencies += "com.typesafe" % "config" % "1.4.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.17.1"


assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
}