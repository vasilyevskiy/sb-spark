name := "filter"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.7" % Provided,
  "org.apache.spark" %% "spark-sql" % "2.4.7" % Provided
)
