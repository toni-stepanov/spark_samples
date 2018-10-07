name := "Spark samples"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0"
libraryDependencies += "org.apache.bahir" % "spark-streaming-twitter_2.11" % "2.1.0"
libraryDependencies += "joda-time" % "joda-time" % "2.9.9"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"



