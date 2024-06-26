name := "Invoice"
version := "0.1"
scalaVersion := "2.12.10"
autoScalaLibrary := false
val sparkVersion = "3.0.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.jfree" % "jfreechart" % "1.5.3",
  "org.jfree" % "jcommon" % "1.0.23"
)

libraryDependencies ++= sparkDependencies