package com.github.chumper.etcd

import com.spotify.docker.client.DefaultDockerClient
import com.whisk.docker.DockerFactory
import com.whisk.docker.impl.spotify.SpotifyDockerFactory
import com.whisk.docker.scalatest.DockerTestKit
import org.scalatest._
import util.EtcdService

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.postfixOps

/**
  * Requires a running etcd on standard port on localhost
  */
class EtcdWatchTest extends AsyncFunSuite with BeforeAndAfter with DockerTestKit with EtcdService {

  override def exposedEtcdPort: Int = 2382

  implicit val executor: ExecutionContext = ExecutionContext.fromExecutor(null)

  var etcd: Etcd = _

  before {
    etcd = Etcd(port = exposedEtcdPort)
  }

  test("Etcd can watch a key") {
    val p = Promise[Assertion]

    for {
     r1 <- etcd.watch.key("12345") { resp =>
       p.success(assert(true))
     }
      r2 <- etcd.kv.putString("12345", "12345")
      r3 <- Future {etcd.watch.cancel(r1)}
    } yield true
    p.future
  }

  test("Etcd can watch a key with prefix") {
    val p = Promise[Assertion]
    for {
      r1 <- etcd.watch.prefix("12345") { resp =>
        p.success(assert(true))
      }
      r2 <- etcd.kv.putString("12345.asdasd", "12345")
      r3 <- Future { etcd.watch.cancel(r1) }
    } yield true
    p.future
  }

  override implicit def dockerFactory: DockerFactory =
    new SpotifyDockerFactory(DefaultDockerClient.fromEnv().build())
}