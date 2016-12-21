package com.github.chumper.etcd

import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import com.google.protobuf.ByteString
import etcdserverpb.rpc.KVGrpc.KVStub
import etcdserverpb.rpc.LeaseGrpc.LeaseStub
import etcdserverpb.rpc.WatchGrpc.WatchStub
import etcdserverpb.rpc._
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import scala.concurrent.{Future, Promise}

/**
  * Main trait for access to all etcd operations
  */
class Etcd(address: String, port: Int, plainText: Boolean = true) {

  private val builder: ManagedChannelBuilder[_ <: ManagedChannelBuilder[_]] = ManagedChannelBuilder.forAddress(address, port)
  if (plainText) {
    builder.usePlaintext(true)
  }
  private val channel: ManagedChannel = builder.build()

  val kv = new EtcdKv(KVGrpc.stub(channel))
  val lease = new EtcdLease(LeaseGrpc.stub(channel))
  val watch = new EtcdWatch(WatchGrpc.stub(channel))
}

object Etcd {
  def apply(address: String = "localhost", port: Int = 2379) = new Etcd(address, port)
}

class EtcdKv(stub: KVStub) {

  def putString(key: String, value: String, lease: Long = 0, previousKey: Boolean = false): Future[PutResponse] = {
    stub.put(PutRequest(
      key = ByteString.copyFromUtf8(key),
      value = ByteString.copyFromUtf8(value),
      lease = lease,
      prevKv = previousKey
    ))
  }

  def put(key: String, value: Array[Byte], lease: Long = 0, previousKey: Boolean = false): Future[PutResponse] = {
    stub.put(PutRequest(
      key = ByteString.copyFromUtf8(key),
      value = ByteString.copyFrom(value),
      lease = lease,
      prevKv = previousKey
    ))
  }

  /**
    * Will get all keys currently set
    *
    * @param keysOnly will return only the keys or also the values
    */
  def keys(keysOnly: Boolean = true): Future[RangeResponse] = {
    stub.range(RangeRequest(
      key = ByteString.copyFromUtf8("\0"),
      rangeEnd = ByteString.copyFromUtf8("\0"),
      keysOnly = keysOnly
    ))
  }

  def get(key: String): Future[RangeResponse] = {
    stub.range(RangeRequest(
      key = ByteString.copyFromUtf8(key)
    ))
  }

  def greater(key: String): Future[RangeResponse] = {
    stub.range(RangeRequest(
      key = ByteString.copyFromUtf8(key),
      rangeEnd = ByteString.copyFromUtf8("\0")
    ))
  }

  def prefix(key: String): Future[RangeResponse] = {
    val byteKey = ByteString.copyFromUtf8(key)

    def getBitIncreasedKey = {
      val lastBit = byteKey.byteAt(byteKey.size() - 1) + 1
      val incKey = byteKey.substring(0, byteKey.size() - 1).toByteArray
      val finalKey = incKey :+ lastBit.toByte
      val byteKeyInc = ByteString.copyFrom(finalKey)
      byteKeyInc
    }

    val byteKeyInc: ByteString = getBitIncreasedKey
    stub.range(RangeRequest(
      key = byteKey,
      rangeEnd = byteKeyInc
    ))
  }

  def deleteAll(): Future[DeleteRangeResponse] = {
    stub.deleteRange(DeleteRangeRequest(
      key = ByteString.copyFromUtf8("\0"),
      rangeEnd = ByteString.copyFromUtf8("\0")
    ))
  }

  def delete(key: String): Future[DeleteRangeResponse] = {
    stub.deleteRange(DeleteRangeRequest(
      key = ByteString.copyFromUtf8(key)
    ))
  }

  def deletePrefix(key: String): Future[DeleteRangeResponse] = {
    stub.deleteRange(DeleteRangeRequest(
      key = ByteString.copyFromUtf8("\0"),
      rangeEnd = ByteString.copyFromUtf8("\0")
    ))
  }
}

class EtcdLease(stub: LeaseStub) {

  /**
    * The connection for the keep alive requests and responses
    */
  private var leaseConnection: Option[StreamObserver[LeaseKeepAliveRequest]] = None

  /**
    * A list of callback that we need to call when a response arrives
    */
  private var callbacks: ConcurrentMap[Long, util.HashSet[Promise[LeaseKeepAliveResponse]]] = new ConcurrentHashMap()

  def grant(ttl: Long, id: Long = 0): Future[LeaseGrantResponse] = {
    stub.leaseGrant(LeaseGrantRequest(
      tTL = ttl,
      iD = id
    ))
  }

  def revoke(id: Long): Future[LeaseRevokeResponse] = {
    stub.leaseRevoke(LeaseRevokeRequest(
      iD = id
    ))
  }

  def keepAlive(id: Long): Future[LeaseKeepAliveResponse] = {
    if (leaseConnection.isEmpty) {
      leaseConnection = createKeepAliveConnection
    }
    val p = Promise[LeaseKeepAliveResponse]
    this.synchronized {
      callbacks.putIfAbsent(id, new util.HashSet[Promise[LeaseKeepAliveResponse]]())
      callbacks.get(id).add(p)
    }
    leaseConnection.get.onNext(LeaseKeepAliveRequest(iD = id))
    p.future
  }

  def info(id: Long, getKeys: Boolean = false): Future[LeaseTimeToLiveResponse] = {
    stub.leaseTimeToLive(LeaseTimeToLiveRequest(
      iD = id,
      keys = getKeys
    ))
  }

  private def createKeepAliveConnection = Some(stub.leaseKeepAlive(new StreamObserver[LeaseKeepAliveResponse] {

    override def onError(t: Throwable): Unit = {
      // close connection
      leaseConnection = None
    }

    override def onCompleted(): Unit = {
      leaseConnection = None
    }

    override def onNext(value: LeaseKeepAliveResponse): Unit = {
      // just ignore, callback feature is not implemented yet
      this.synchronized {
        val callers = callbacks.getOrDefault(value.iD, new util.HashSet[Promise[LeaseKeepAliveResponse]]())
        callers.forEach(p => p.success(value))
        callbacks.remove(value.iD)
      }
    }
  }))
}

class EtcdWatch(stub: WatchStub) {

  /**
    * The connection for the keep alive requests and responses
    */
  private var watchConnection: Option[StreamObserver[WatchRequest]] = None

  /**
    * A list of callback that we need to call when a response arrives
    */
  private var callbacks: ConcurrentMap[Long, Option[WatchResponse => Unit]] = new ConcurrentHashMap()

  private var currentWaitingWatchRequest: Option[WatchResponse => Unit] = None

  private def createWatchConnection = Some(stub.watch(new StreamObserver[WatchResponse] {

    override def onError(t: Throwable): Unit = {
      // close connection
      watchConnection = None
    }

    override def onCompleted(): Unit = {
      watchConnection = None
    }

    override def onNext(value: WatchResponse): Unit = {
      // just ignore, callback feature is not implemented yet
      println(value.toString)

      if (value.created) {
        currentWaitingWatchRequest match {
          case Some(callback) => callbacks.put(value.watchId, Some(callback))
            currentWaitingWatchRequest = None
          case None =>
        }
      }
      val callers = callbacks.getOrDefault(value.watchId, None)
      callers match {
        case Some(callback) => callback.apply(value)
        case None =>
      }
    }
  }))

  def key(id: String)(callback: WatchResponse => Unit) = {
    // we need to block on this because etcd does NOT support concurrent watch creations on a single stream
    // https://github.com/coreos/etcd/issues/7036

    this.synchronized {
      if (watchConnection.isEmpty) {
        watchConnection = createWatchConnection
      }

      currentWaitingWatchRequest = Some(callback)

      watchConnection.get.onNext(
        WatchRequest().withCreateRequest(WatchCreateRequest(
          key = ByteString.copyFromUtf8(id)
        ))
      )
    }
  }

  def prefix(id: String)(callback: WatchResponse => Unit) = {
    // we need to block on this because etcd does NOT support concurrent watch creations on a single stream
    // https://github.com/coreos/etcd/issues/7036

    this.synchronized {
      if (watchConnection.isEmpty) {
        watchConnection = createWatchConnection
      }

      currentWaitingWatchRequest = Some(callback)

      watchConnection.get.onNext(
        WatchRequest().withCreateRequest(WatchCreateRequest(
          key = ByteString.copyFromUtf8(id)
        ))
      )
    }
  }

  def cancel(watchId: Long): Unit = {
    // remove from list
    this.synchronized {
      callbacks.remove(watchId)
      watchConnection.get.onNext(
        WatchRequest().withCancelRequest(WatchCancelRequest(
          watchId = watchId
        ))
      )
    }
  }

}