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

import scala.concurrent.Future

/**
  * Main trait for access to all etcd operations
  */
class Etcd(address: String, port: Int, plainText: Boolean = true) {

  private val builder: ManagedChannelBuilder[_ <: ManagedChannelBuilder[_]] = ManagedChannelBuilder.forAddress(address, port)
  if(plainText) {
    builder.usePlaintext(true)
  }
  private val channel: ManagedChannel = builder.build()

  val kv = new EtcdKv(KVGrpc.stub(channel))
  val lease = new EtcdLease(LeaseGrpc.stub(channel))
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

  def deleteAll() : Future[DeleteRangeResponse] = {
    stub.deleteRange(DeleteRangeRequest(
      key =  ByteString.copyFromUtf8("\0"),
      rangeEnd = ByteString.copyFromUtf8("\0")
    ))
  }

  def delete(key: String) : Future[DeleteRangeResponse] = {
    stub.deleteRange(DeleteRangeRequest(
      key =  ByteString.copyFromUtf8(key)
    ))
  }

  def deletePrefix(key: String) : Future[DeleteRangeResponse] = {
    stub.deleteRange(DeleteRangeRequest(
      key =  ByteString.copyFromUtf8("\0"),
      rangeEnd = ByteString.copyFromUtf8("\0")
    ))
  }
}

class EtcdLease(stub: LeaseStub) {

  /**
    * The connection for the keep alive requests and responses
    */
  private var leaseConnection : Option[StreamObserver[LeaseKeepAliveRequest]] = None

  /**
    * A list of callback that we need to call when a response arrives
    */
  private var callbacks: ConcurrentMap[Long, util.HashSet[LeaseKeepAliveResponse => Unit]] = new ConcurrentHashMap()

  def grant(ttl: Long, id: Long = 0) : Future[LeaseGrantResponse] = {
    stub.leaseGrant(LeaseGrantRequest(
      tTL = ttl,
      iD = id
    ))
  }

  def revoke(id: Long) : Future[LeaseRevokeResponse] = {
    stub.leaseRevoke(LeaseRevokeRequest(
      iD = id
    ))
  }

  def keepAlive(id: Long, f: LeaseKeepAliveResponse => Unit = { _ => }) : Unit = {
    if(leaseConnection.isEmpty) {
      leaseConnection = createKeepAliveConnection
    }
    this.synchronized {
      callbacks.putIfAbsent(id, new util.HashSet[(LeaseKeepAliveResponse) => Unit]())
      callbacks.get(id).add(f)
    }
    leaseConnection.get.onNext(LeaseKeepAliveRequest(iD = id))
  }

  def info(id: Long, getKeys: Boolean = false) : Future[LeaseTimeToLiveResponse] = {
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
        val callers = callbacks.getOrDefault(value.iD, new util.HashSet[(LeaseKeepAliveResponse) => Unit]())
        callers.forEach { _.apply(value) }
        callbacks.remove(value.iD)
      }
    }
  }))
}

class EtcdWatch(stub: WatchStub) {

//  /**
//    * The connection for the keep alive requests and responses
//    */
//  private var leaseConnection : Option[StreamObserver[LeaseKeepAliveRequest]] = None
//
//  /**
//    * A list of callback that we need to call when a response arrives
//    */
//  private var callbacks: ConcurrentMap[Long, util.HashSet[WatchResponse => Unit]] = new ConcurrentHashMap()
//
//  private def createWatchConnection = Some(stub.watch(new StreamObserver[WatchResponse] {
//
//    override def onError(t: Throwable): Unit = {
//      // close connection
//      leaseConnection = None
//    }
//
//    override def onCompleted(): Unit = {
//      leaseConnection = None
//    }
//
//    override def onNext(value: WatchResponse): Unit = {
//      // just ignore, callback feature is not implemented yet
//      this.synchronized {
//        val callers = callbacks.getOrDefault(value.iD, new util.HashSet[(LeaseKeepAliveResponse) => Unit]())
//        callers.forEach { _.apply(value) }
//        callbacks.remove(value.iD)
//      }
//    }
//  }))

}