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