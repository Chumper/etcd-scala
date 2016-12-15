package com.github.chumper.etcd

import com.google.protobuf.ByteString
import etcdserverpb.rpc.KVGrpc.KVStub
import etcdserverpb.rpc.LeaseGrpc.LeaseStub
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

  private var leaseConnection : Option[StreamObserver[LeaseKeepAliveRequest]] = None

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

  def keepAlive(id: Long) : Unit = {
    if(leaseConnection.isEmpty) {
      leaseConnection = Some(stub.leaseKeepAlive(new StreamObserver[LeaseKeepAliveResponse] {

        override def onError(t: Throwable): Unit = {
          // close connection
          leaseConnection = None
        }

        override def onCompleted(): Unit = {
          leaseConnection = None
        }

        override def onNext(value: LeaseKeepAliveResponse): Unit = {
          // just ignore, callback feature is not implemented yet
        }
      }))
    }
    leaseConnection.get.onNext(LeaseKeepAliveRequest(iD = id))
  }

  def info(id: Long, getKeys: Boolean = false) : Future[LeaseTimeToLiveResponse] = {
    stub.leaseTimeToLive(LeaseTimeToLiveRequest(
      iD = id,
      keys = getKeys
    ))
  }
}