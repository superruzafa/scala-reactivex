name := "scala-reactivex"
scalaVersion := "2.11.12"
scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-value-discard",
  "-Xfuture",
  "-Xexperimental"
)

lazy val example = project
lazy val async = project
lazy val actorbintree = project
lazy val kvstore = project
lazy val protocols = project

lazy val root = (project in file("."))
  .aggregate(example, async, actorbintree, kvstore, protocols)
