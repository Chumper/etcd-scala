lazy val root = Project("etcd3-scala", file(".")) dependsOn protoProject
lazy val protoProject = RootProject(uri("git://github.com/Chumper/etcd3-proto.git#0.1.0"))