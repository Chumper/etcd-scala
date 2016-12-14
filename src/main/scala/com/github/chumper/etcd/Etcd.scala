package com.github.chumper.etcd

import com.google.protobuf.ByteString
import etcdserverpb.rpc.KVGrpc.KVStub
import etcdserverpb.rpc._
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import scala.concurrent.Future

/**
  * Main trait for access to all etcd operations
  */
class Etcd(address: String, port: Int, plainText: Boolean = true) extends EtcdPut with EtcdRange with EtcdDelete {

  val builder: ManagedChannelBuilder[_ <: ManagedChannelBuilder[_]] = ManagedChannelBuilder.forAddress(address, port)
  if(plainText) {
    builder.usePlaintext(true)
  }
  val channel: ManagedChannel = builder
    .build()

  val kvStub: KVStub = KVGrpc.stub(channel)
}

object Etcd {
  def apply(address: String = "localhost", port: Int = 2379) = new Etcd(address, port)
}

trait EtcdPut {

  implicit val kvStub: KVStub

  /**
    * Will set a single string value to a given key
    *
    * @param key         The key to set the value on
    * @param value       the value to set
    * @param lease       the lease to associate the key with
    * @param previousKey if the previous key should be returned
    * @return
    */
  def putString(key: String, value: String, lease: Long = 0, previousKey: Boolean = false): Future[PutResponse] = {
    kvStub.put(PutRequest(
      key = ByteString.copyFromUtf8(key),
      value = ByteString.copyFromUtf8(value),
      lease = lease,
      prevKv = previousKey
    ))
  }

  def put(key: String, value: Array[Byte], lease: Long = 0, previousKey: Boolean = false): Future[PutResponse] = {
    kvStub.put(PutRequest(
      key = ByteString.copyFromUtf8(key),
      value = ByteString.copyFrom(value),
      lease = lease,
      prevKv = previousKey
    ))
  }
}

trait EtcdRange {

  implicit val kvStub: KVStub

  /**
    * Will get all keys currently set
    *
    * @param keysOnly will return only the keys or also the values
    */
  def keys(keysOnly: Boolean = true): Future[RangeResponse] = {
    kvStub.range(RangeRequest(
      key = ByteString.copyFromUtf8("\0"),
      rangeEnd = ByteString.copyFromUtf8("\0"),
      keysOnly = keysOnly
    ))
  }

  def get(key: String): Future[RangeResponse] = {
    kvStub.range(RangeRequest(
      key = ByteString.copyFromUtf8(key)
    ))
  }

  def greater(key: String): Future[RangeResponse] = {
    kvStub.range(RangeRequest(
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
    kvStub.range(RangeRequest(
      key = byteKey,
      rangeEnd = byteKeyInc
    ))
  }
}

trait EtcdDelete {
  implicit val kvStub: KVStub

  def deleteAll() : Future[DeleteRangeResponse] = {
    kvStub.deleteRange(DeleteRangeRequest(
      key =  ByteString.copyFromUtf8("\0"),
      rangeEnd = ByteString.copyFromUtf8("\0")
    ))
  }

  def delete(key: String) : Future[DeleteRangeResponse] = {
    kvStub.deleteRange(DeleteRangeRequest(
      key =  ByteString.copyFromUtf8(key)
    ))
  }

  def deletePrefix(key: String) : Future[DeleteRangeResponse] = {
    kvStub.deleteRange(DeleteRangeRequest(
      key =  ByteString.copyFromUtf8("\0"),
      rangeEnd = ByteString.copyFromUtf8("\0")
    ))
  }
}