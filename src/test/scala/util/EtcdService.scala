package util

import com.whisk.docker.{DockerContainer, DockerKit, DockerReadyChecker}

import scala.language.postfixOps

trait EtcdService extends DockerKit {

  val DefaultEtcdPort = 2379

  val etcdContainer: DockerContainer = DockerContainer("alfatraining/etcd3")
    .withPorts(DefaultEtcdPort -> Some(DefaultEtcdPort))
    .withReadyChecker(
      DockerReadyChecker
        .LogLineContains("ready to serve client requests")
    )

  abstract override def dockerContainers: List[DockerContainer] =
    etcdContainer :: super.dockerContainers

}
