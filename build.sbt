name := "etcd3-scala"

version := "0.1.0"

scalaVersion := "2.12.0"

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

// If you need scalapb/scalapb.proto or anything from
// google/protobuf/*.proto
libraryDependencies += "com.trueaccord.scalapb" %% "scalapb-runtime" % com.trueaccord.scalapb.compiler.Version.scalapbVersion % "protobuf"
libraryDependencies += "com.trueaccord.scalapb" %% "scalapb-runtime-grpc" % com.trueaccord.scalapb.compiler.Version.scalapbVersion
libraryDependencies += "io.grpc" % "grpc-all" % "1.0.2"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.8"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "org.slf4j" % "jul-to-slf4j" % "1.7.22"
libraryDependencies += "org.codehaus.groovy" % "groovy-all" % "2.4.7"

//libraryDependencies += "com.spotify" % "docker-client" % "6.1.1"
//libraryDependencies += "com.github.whisklabs" % "docker-it-scala" % "v0.9.0-RC2"
libraryDependencies += "com.whisk" %% "docker-testkit-scalatest" % "0.9.0-RC2" % "test"
libraryDependencies += "com.whisk" %% "docker-testkit-impl-spotify" % "0.9.0-RC2" % "test"