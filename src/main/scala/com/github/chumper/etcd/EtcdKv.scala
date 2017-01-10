package com.github.chumper.etcd

import com.github.chumper.grpc
import com.google.protobuf.ByteString
import etcdserverpb.rpc.KVGrpc.KVStub
import etcdserverpb.rpc._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by n.plaschke on 03/01/2017.
  */
class EtcdKv(private var stub: KVStub)(implicit val ec: ExecutionContext) {

  def withToken(token: String): Unit = {
    this.stub = KVGrpc.stub(stub.getChannel).withCallCredentials(grpc.EtcdTokenCredentials(token))
  }

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
