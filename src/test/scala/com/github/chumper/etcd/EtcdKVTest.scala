package com.github.chumper.etcd

import com.spotify.docker.client.{DefaultDockerClient, DockerClient}
import com.whisk.docker.DockerFactory
import com.whisk.docker.impl.spotify.SpotifyDockerFactory
import com.whisk.docker.scalatest.DockerTestKit
import org.scalatest._
import util.EtcdService

import scala.concurrent.{ExecutionContext, Promise}
import scala.language.postfixOps

/**
  * Requires docker api available on localhost
  */
class EtcdKVTest extends AsyncFunSuite with BeforeAndAfter with DockerTestKit with EtcdService {

  override def exposedEtcdPort: Int = 2380

  implicit val executor: ExecutionContext = ExecutionContext.fromExecutor(null)

  var etcd: Etcd = _

  before {
    etcd = Etcd(port = exposedEtcdPort)
  }

  test("Etcd can set a string") {
    for {
      r1 <- etcd.kv.putString("foo1", "bar")
      r2 <- etcd.kv.get("foo1")
    } yield assert(r2.kvs.head.value.toStringUtf8 === "bar")
  }

  test("Etcd can set a byte value") {
    for {
      r1 <- etcd.kv.put("foo2", Array(123.toByte))
      r2 <- etcd.kv.get("foo2")
    } yield assert(r2.kvs.head.value.byteAt(0) === 123)
  }

  test("Etcd can get all keys") {
    for {
      r1 <- etcd.kv.putString("foo3", "bar")
      r2 <- etcd.kv.keys()
    } yield assert(r2.kvs.exists(v => v.key.toStringUtf8 == "foo3"))
  }

  test("Etcd can get all keys with values") {
    for {
      r1 <- etcd.kv.putString("foo4", "bar")
      r2 <- etcd.kv.keys(keysOnly = false)
    } yield assert(r2.kvs.exists(v => v.key.toStringUtf8 == "foo4" && v.value.toStringUtf8 == "bar"))
  }

  test("Etcd can get all prefixes with values") {
    for {
      r1 <- etcd.kv.putString("foo5", "bar")
      r2 <- etcd.kv.putString("foo6", "bar")
      r3 <- etcd.kv.putString("goo1", "bar")
      r4 <- etcd.kv.prefix("foo")
    } yield {
      assert(r4.kvs.exists(v => v.key.toStringUtf8 == "foo5" && v.value.toStringUtf8 == "bar"))
      assert(r4.kvs.exists(v => v.key.toStringUtf8 == "foo6" && v.value.toStringUtf8 == "bar"))
      assert(!r4.kvs.exists(v => v.key.toStringUtf8 == "goo1" && v.value.toStringUtf8 == "bar"))
    }
  }

  test("Etcd can get all greater with values") {
    for {
      r1 <- etcd.kv.putString("hoo5", "bar")
      r2 <- etcd.kv.putString("hoo6", "bar")
      r3 <- etcd.kv.putString("ioo1", "bar")
      r4 <- etcd.kv.greater("hoo")
    } yield {
      assert(r4.kvs.exists(v => v.key.toStringUtf8 == "hoo5" && v.value.toStringUtf8 == "bar"))
      assert(r4.kvs.exists(v => v.key.toStringUtf8 == "hoo6" && v.value.toStringUtf8 == "bar"))
      assert(r4.kvs.exists(v => v.key.toStringUtf8 == "ioo1" && v.value.toStringUtf8 == "bar"))
    }
  }

  test("Etcd can delete all keys") {
    for {
      r1 <- etcd.kv.putString("foo7", "bar")
      r2 <- etcd.kv.deleteAll()
    } yield assert(r2.deleted > 0)
  }

  test("Etcd can delete specific keys") {
    for {
      r1 <- etcd.kv.putString("foo8", "bar")
      r2 <- etcd.kv.delete("foo8")
    } yield assert(r2.deleted > 0)
  }

  override implicit def dockerFactory: DockerFactory =
    new SpotifyDockerFactory(DefaultDockerClient.fromEnv().build())
}