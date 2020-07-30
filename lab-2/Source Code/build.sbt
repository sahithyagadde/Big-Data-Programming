name := "lab2"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-mllib" % "2.3.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.3.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.1",


)
