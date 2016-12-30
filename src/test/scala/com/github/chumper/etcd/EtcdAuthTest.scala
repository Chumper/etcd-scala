package com.github.chumper.etcd

import com.spotify.docker.client.DefaultDockerClient
import com.typesafe.scalalogging.Logger
import com.whisk.docker.DockerFactory
import com.whisk.docker.impl.spotify.SpotifyDockerFactory
import com.whisk.docker.scalatest.DockerTestKit
import io.grpc.StatusRuntimeException
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import util.EtcdService

import scala.concurrent.duration.DurationLong
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

/**
  * Requires a running etcd on standard port on localhost
  */
class EtcdAuthTest extends AsyncFunSuite with BeforeAndAfter with DockerTestKit with EtcdService {

  implicit val executor: ExecutionContext = ExecutionContext.fromExecutor(null)

  var etcd: Etcd = _

  val log: Logger = Logger[EtcdAuthTest]

  def setUpAuthentication(): Future[Boolean] = {
    // create root user
    for {
      r1 <- etcd.auth.addUser("root", "root")
      r2 <- etcd.auth.addRole("root")
      r3 <- etcd.auth.grantRole("root", "root")
    } yield true
  }

  def disableAuth(): Future[Boolean] = {
    val e = Etcd().withAuth("root", "root")
    for {
      r2 <- e.auth.disable()
    } yield true
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    etcd = Etcd()
    Await.result(setUpAuthentication(), 5 seconds)
  }

  before {
    etcd = Etcd()
  }

  test("Etcd can login") {
    for {
      r0 <- etcd.auth.enable()
      r1 <- etcd.auth.authenticate("root", "root")
      r2 <- Future { etcd.withToken(r1) }
      r3 <- etcd.kv.putString("foo", "bar")
      r4 <- etcd.auth.disable()
    } yield assert(r1 != null && !r1.isEmpty)
  }

  test("Etcd can enable auth") {
      for {
        r1 <- etcd.auth.enable()
        r2 <- {
          val login = etcd.auth.authenticate("foo", "bar")
          ScalaFutures.whenReady(login.failed) { e =>
            assert(e.isInstanceOf[StatusRuntimeException])
          }
        }
        r3 <- disableAuth()
      } yield assert(true)
  }

  test("Etcd can disable auth") {
    for {
      r1 <- etcd.auth.enable()
      r2 <- etcd.auth.authenticate("root", "root")
      r3 <- Future { etcd.withToken(r2) }
      r4 <- etcd.auth.disable()
    } yield assert(true)
  }

  test("Etcd can manage users and roles") {
    for {
      r1 <- etcd.auth.getUser("root")
      r2 <- etcd.auth.getRole("root")
      r3 <- etcd.auth.deleteRole("root")
      r4 <- etcd.auth.deleteUser("root")
    } yield assert(r1.roles.contains("root"))
  }

  override implicit def dockerFactory: DockerFactory =
    new SpotifyDockerFactory(DefaultDockerClient.fromEnv().build())
}