package util

import com.whisk.docker.{DockerContainer, DockerKit, DockerReadyChecker}

import scala.language.postfixOps

trait EtcdService extends DockerKit {

  def defaultEtcdPort = 2379
  def exposedEtcdPort = 2379

  val etcdContainer: DockerContainer = DockerContainer("alfatraining/etcd3")
    .withPorts(defaultEtcdPort -> Some(exposedEtcdPort))
    .withReadyChecker(
      DockerReadyChecker
        .LogLineContains("ready to serve client requests")
    )

  abstract override def dockerContainers: List[DockerContainer] =
    etcdContainer :: super.dockerContainers

}
