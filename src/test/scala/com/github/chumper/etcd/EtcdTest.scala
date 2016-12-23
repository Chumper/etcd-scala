package com.github.chumper.etcd

import java.util.concurrent.Future

import org.scalatest.{Assertion, AsyncFunSuite, BeforeAndAfter, ParallelTestExecution}

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.DurationDouble
import scala.language.postfixOps

/**
  * Requires a running etcd on standard port on localhost
  */
class EtcdTest extends AsyncFunSuite with BeforeAndAfter with ParallelTestExecution {

  var etcd: Etcd = _

  before {
    etcd = Etcd()
  }

  test("Etcd can set a string") {
    etcd.kv.putString("foo1", "bar").map { resp =>
      etcd.kv.get("foo1") map { data =>
        assert(data.kvs.head.value.toStringUtf8 === "bar")
      }
    }.flatten
  }

  test("Etcd can set a byte value") {
    etcd.kv.put("foo2", Array(123.toByte)).map { resp =>
      etcd.kv.get("foo2") map { data =>
        assert(data.kvs.head.value.byteAt(0) === 123)
      }
    }.flatten
  }

  test("Etcd can get all keys") {
    etcd.kv.putString("foo3", "bar").map { resp =>
      etcd.kv.keys() map { data =>
        assert(data.kvs.exists(v => v.key.toStringUtf8 == "foo3"))
      }
    }.flatten
  }

  test("Etcd can get all keys with values") {
    etcd.kv.putString("foo4", "bar").map { resp =>
      etcd.kv.keys(keysOnly = false) map { data =>
        assert(data.kvs.exists(v => v.key.toStringUtf8 == "foo4" && v.value.toStringUtf8 == "bar"))
      }
    }.flatten
  }

  test("Etcd can get all prefixes with values") {
    etcd.kv.putString("foo5", "bar").map { resp =>
      etcd.kv.putString("foo6", "bar").map { resp2 =>
        etcd.kv.putString("goo1", "bar").map { resp3 =>
          etcd.kv.prefix("foo") map { data =>
            assert(data.kvs.exists(v => v.key.toStringUtf8 == "foo5" && v.value.toStringUtf8 == "bar"))
            assert(data.kvs.exists(v => v.key.toStringUtf8 == "foo6" && v.value.toStringUtf8 == "bar"))
            assert(!data.kvs.exists(v => v.key.toStringUtf8 == "goo1" && v.value.toStringUtf8 == "bar"))
          }
        }.flatten
      }.flatten
    }.flatten
  }

  test("Etcd can get all greater with values") {
    etcd.kv.putString("hoo5", "bar").map { resp =>
      etcd.kv.putString("hoo6", "bar").map { resp2 =>
        etcd.kv.putString("ioo1", "bar").map { resp3 =>
          etcd.kv.greater("hoo") map { data =>
            assert(data.kvs.exists(v => v.key.toStringUtf8 == "hoo5" && v.value.toStringUtf8 == "bar"))
            assert(data.kvs.exists(v => v.key.toStringUtf8 == "hoo6" && v.value.toStringUtf8 == "bar"))
            assert(data.kvs.exists(v => v.key.toStringUtf8 == "ioo1" && v.value.toStringUtf8 == "bar"))
          }
        }.flatten
      }.flatten
    }.flatten
  }

  test("Etcd can delete all keys") {
    etcd.kv.putString("foo7", "bar").map { resp =>
      etcd.kv.deleteAll() map { data =>
        assert(data.deleted > 0)
      }
    }.flatten
  }

  test("Etcd can login") {
    etcd.auth.authenticate("root", "root").map { resp =>
      assert(true)
    }
  }

  test("Etcd can delete specific keys") {
    etcd.kv.putString("foo8", "bar").map { resp =>
      etcd.kv.delete("foo8") map { data =>
        assert(data.deleted > 0)
      }
    }.flatten
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

  test("Etcd can watch a key") {
    val p = Promise[Assertion]
    var watchId = 0l
    etcd.watch.key("12345") { resp =>
      watchId = resp.watchId
      p.success(assert(true))
    }
    etcd.kv.putString("12345", "12345") map { resp =>
      etcd.watch.cancel(watchId)
    }
    p.future
  }

  test("Etcd can watch a key with prefix") {
    val p = Promise[Assertion]
    var watchId = 0l
    etcd.watch.prefix("12345") { resp =>
      watchId = resp.watchId
      p.success(assert(true))
    }
    etcd.kv.putString("12345.asdasd", "12345") map { resp =>
      etcd.watch.cancel(watchId)
    }
    p.future
  }
}