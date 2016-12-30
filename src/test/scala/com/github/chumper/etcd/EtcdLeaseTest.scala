package com.github.chumper.etcd

import com.spotify.docker.client.DefaultDockerClient
import com.whisk.docker.DockerFactory
import com.whisk.docker.impl.spotify.SpotifyDockerFactory
import com.whisk.docker.scalatest.DockerTestKit
import org.scalatest._
import util.EtcdService

import scala.concurrent.{ExecutionContext, Promise}
import scala.language.postfixOps

/**
  * Requires a running etcd on standard port on localhost
  */
class EtcdLeaseTest extends AsyncFunSuite with BeforeAndAfter with DockerTestKit with EtcdService {

  override def exposedEtcdPort: Int = 2381

  implicit val executor: ExecutionContext = ExecutionContext.fromExecutor(null)

  var etcd: Etcd = _

  before {
    etcd = Etcd(port = exposedEtcdPort)
  }

  test("Etcd can grant a lease") {
    etcd.lease.grant(10).map { resp =>
      assert(resp.tTL === 10)
    }
  }

  test("Etcd can grant and revoke a lease") {
    for {
      r1 <- etcd.lease.grant(10)
      r2 <- etcd.lease.revoke(r1.iD)
    } yield assert(r1.header !== None)
  }

  test("Etcd can grant and keep alive a lease") {
    for {
      r1 <- etcd.lease.grant(10)
      r2 <- etcd.lease.keepAlive(r1.iD)
    } yield assert(true)
  }

  test("Etcd can grant and keep alive a lease with a future") {
    for {
      r1 <- etcd.lease.grant(10)
      r2 <- etcd.lease.keepAlive(r1.iD)
    } yield assert(r2.iD === r1.iD && r2.tTL === r1.tTL)
  }

  override implicit def dockerFactory: DockerFactory =
    new SpotifyDockerFactory(DefaultDockerClient.fromEnv().build())
}