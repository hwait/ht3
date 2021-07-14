ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "0.1.0"
ThisBuild / organization     := "ru.otus"
ThisBuild / organizationName := "bigdataml"

val kafkaVersion = "2.8.0"
val sparkVersion = "3.1.1"

libraryDependencies  ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
    // "org.apache.kafka" % "kafka-streams" % kafkaVersion,
    // "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
    "org.apache.logging.log4j" % "log4j-core" % "2.4.1",  
    "org.scalatest" %% "scalatest" % "3.2.8",
    "com.typesafe" % "config" % "1.4.1"
    )
