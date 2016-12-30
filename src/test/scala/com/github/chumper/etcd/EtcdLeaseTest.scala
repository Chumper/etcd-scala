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
class EtcdLeaseTest extends AsyncFunSuite with BeforeAndAfterAll with BeforeAndAfter with DockerTestKit with EtcdService {

  implicit val executor: ExecutionContext = ExecutionContext.fromExecutor(null)

  var etcd: Etcd = _

  before {
    etcd = Etcd()
  }

  test("Etcd can grant a lease") {
    etcd.lease.grant(10).map { resp =>
      assert(resp.tTL === 10)
    }
  }

  test("Etcd can grant and revoke a lease") {
    etcd.lease.grant(10).map { resp =>
      etcd.lease.revoke(resp.iD) map { data =>
        assert(data.header !== None)
      }
    }.flatten
  }

  test("Etcd can grant and keep alive a lease") {
    etcd.lease.grant(10).map { resp =>
      etcd.lease.keepAlive(resp.iD)
      assert(true)
    }
  }

  test("Etcd can grant and keep alive a lease with a future") {
    etcd.lease.grant(10).map { resp =>
      etcd.lease.keepAlive(resp.iD) map { t =>
        assert(t.iD === resp.iD && t.tTL === resp.tTL)
      }
    }.flatten
  }

  override implicit def dockerFactory: DockerFactory =
    new SpotifyDockerFactory(DefaultDockerClient.fromEnv().build())
}