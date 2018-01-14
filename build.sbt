name := "etcd3-scala"

version := "0.1.0"

scalaVersion := "2.12.4"
crossScalaVersions := Seq("2.11.8", "2.12.4")

parallelExecution in Test := false

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

// If you need scalapb/scalapb.proto or anything from
// google/protobuf/*.proto
libraryDependencies += "com.trueaccord.scalapb" %% "scalapb-runtime" % com.trueaccord.scalapb.compiler.Version.scalapbVersion % "protobuf"
libraryDependencies += "com.trueaccord.scalapb" %% "scalapb-runtime-grpc" % com.trueaccord.scalapb.compiler.Version.scalapbVersion
libraryDependencies += "io.grpc" % "grpc-all" % "1.8.0"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.4"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
libraryDependencies += "org.slf4j" % "jul-to-slf4j" % "1.7.25"
libraryDependencies += "org.codehaus.groovy" % "groovy-all" % "2.4.13"

//libraryDependencies += "com.spotify" % "docker-client" % "6.1.1"
//libraryDependencies += "com.github.whisklabs" % "docker-it-scala" % "v0.9.5"
libraryDependencies += "com.whisk" %% "docker-testkit-scalatest" % "0.9.5" % "test"
libraryDependencies += "com.whisk" %% "docker-testkit-impl-spotify" % "0.9.5" % "test"