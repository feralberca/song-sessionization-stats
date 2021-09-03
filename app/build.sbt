name := "song-sessionization-stats"
version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.3",
  "org.scalatest" %% "scalatest" % "3.0.7" % Test
)
