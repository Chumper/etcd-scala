 package com.github.chumper.etcd

import org.scalatest.{AsyncFunSuite, BeforeAndAfter, ParallelTestExecution}

/**
  * Requires a running etcd on standard port on localhost
  */
class EtcdTest extends AsyncFunSuite with BeforeAndAfter with ParallelTestExecution {

  var etcd: Etcd = _

  before {
    etcd = Etcd()
  }

  test("Etcd can set a string") {
    etcd.putString("foo1", "bar").map { resp =>
      etcd.get("foo1") map { data =>
        assert(data.kvs.head.value.toStringUtf8 === "bar")
      }
    }.flatten
  }

  test("Etcd can set a byte value") {
    etcd.put("foo2", Array(123.toByte)).map { resp =>
      etcd.get("foo2") map { data =>
        assert(data.kvs.head.value.byteAt(0) === 123)
      }
    }.flatten
  }

  test("Etcd can get all keys") {
    etcd.putString("foo3", "bar").map { resp =>
      etcd.keys() map { data =>
        assert(data.kvs.exists(v => v.key.toStringUtf8 == "foo3"))
      }
    }.flatten
  }

  test("Etcd can get all keys with values") {
    etcd.putString("foo4", "bar").map { resp =>
      etcd.keys(keysOnly = false) map { data =>
        assert(data.kvs.exists(v => v.key.toStringUtf8 == "foo4" && v.value.toStringUtf8 == "bar"))
      }
    }.flatten
  }

  test("Etcd can get all prefixes with values") {
    etcd.putString("foo5", "bar").map { resp =>
      etcd.putString("foo6", "bar").map { resp2 =>
        etcd.putString("goo1", "bar").map { resp3 =>
          etcd.prefix("foo") map { data =>
            assert(data.kvs.exists(v => v.key.toStringUtf8 == "foo5" && v.value.toStringUtf8 == "bar"))
            assert(data.kvs.exists(v => v.key.toStringUtf8 == "foo6" && v.value.toStringUtf8 == "bar"))
            assert(!data.kvs.exists(v => v.key.toStringUtf8 == "goo1" && v.value.toStringUtf8 == "bar"))
          }
        }.flatten
      }.flatten
    }.flatten
  }

  test("Etcd can get all greater with values") {
    etcd.putString("hoo5", "bar").map { resp =>
      etcd.putString("hoo6", "bar").map { resp2 =>
        etcd.putString("ioo1", "bar").map { resp3 =>
          etcd.greater("hoo") map { data =>
            assert(data.kvs.exists(v => v.key.toStringUtf8 == "hoo5" && v.value.toStringUtf8 == "bar"))
            assert(data.kvs.exists(v => v.key.toStringUtf8 == "hoo6" && v.value.toStringUtf8 == "bar"))
            assert(data.kvs.exists(v => v.key.toStringUtf8 == "ioo1" && v.value.toStringUtf8 == "bar"))
          }
        }.flatten
      }.flatten
    }.flatten
  }

  test("Etcd can delete all keys") {
    etcd.putString("foo7", "bar").map { resp =>
      etcd.deleteAll() map { data =>
        assert(data.deleted > 0)
      }
    }.flatten
  }

  test("Etcd can delete specific keys") {
    etcd.putString("foo8", "bar").map { resp =>
      etcd.delete("foo8") map { data =>
        assert(data.deleted > 0)
      }
    }.flatten
  }
}
