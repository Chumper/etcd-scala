package com.github.chumper.etcd

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import com.github.chumper.grpc
import com.google.protobuf.ByteString
import etcdserverpb.rpc.WatchGrpc.WatchStub
import etcdserverpb.rpc._
import io.grpc.stub.StreamObserver

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * Created by n.plaschke on 03/01/2017.
  */
class EtcdWatch(private var stub: WatchStub)(implicit val ec: ExecutionContext) {

  def withToken(token: String): Unit = {
    this.stub = WatchGrpc.stub(stub.getChannel).withCallCredentials(grpc.EtcdTokenCredentials(token))
  }

  /**
    * The connection for the keep alive requests and responses
    */
  private var watchConnection: Option[StreamObserver[WatchRequest]] = None

  /**
    * A list of callback that we need to call when a response arrives
    */
  private val callbacks: ConcurrentMap[Long, Option[WatchResponse => Unit]] = new ConcurrentHashMap()

  private var watchRequestQueue: mutable.Queue[Tuple2[Promise[Long], WatchResponse => Unit]] = mutable.Queue()

  private def createWatchConnection = Some(stub.watch(new StreamObserver[WatchResponse] {

    override def onError(t: Throwable): Unit = {
      // close connection
      watchConnection = None
    }

    override def onCompleted(): Unit = {
      watchConnection = None
    }

    override def onNext(value: WatchResponse): Unit = {
      if (value.created) {
        watchRequestQueue match {
          case mutable.Queue(head, rest@_*) =>
            val el = watchRequestQueue.dequeue()
            callbacks.put(value.watchId, Some(el._2))
            el._1.success(value.watchId)
          case _ =>
        }
      }
      val callers = callbacks.getOrDefault(value.watchId, None)
      callers match {
        case Some(callback) => callback.apply(value)
        case None =>
      }
    }
  }))

  def key(id: String)(callback: WatchResponse => Unit): Future[Long] = {
    this.synchronized {
      val p = Promise[Long]
      if (watchConnection.isEmpty) {
        watchConnection = createWatchConnection
      }

      watchRequestQueue += Tuple2(p, callback)

      watchConnection.get.onNext(
        WatchRequest().withCreateRequest(WatchCreateRequest(
          key = ByteString.copyFromUtf8(id)
        ))
      )
      p.future
    }
  }

  def prefix(id: String)(callback: WatchResponse => Unit): Future[Long] = {
    this.synchronized {
      val p = Promise[Long]

      if (watchConnection.isEmpty) {
        watchConnection = createWatchConnection
      }

      watchRequestQueue += Tuple2(p, callback)

      val key = ByteString.copyFromUtf8(id)
      watchConnection.get.onNext(
        WatchRequest().withCreateRequest(WatchCreateRequest(
          key = key,
          rangeEnd = getBitIncreasedKey(key)
        ))
      )
      p.future
    }
  }

  def cancel(watchId: Long): Unit = {
    // remove from list
    this.synchronized {
      watchConnection.get.onNext(
        WatchRequest().withCancelRequest(WatchCancelRequest(
          watchId = watchId
        ))
      )
    }
  }

  def getBitIncreasedKey(byteKey: ByteString) = {
    val lastBit = byteKey.byteAt(byteKey.size() - 1) + 1
    val incKey = byteKey.substring(0, byteKey.size() - 1).toByteArray
    val finalKey = incKey :+ lastBit.toByte
    val byteKeyInc = ByteString.copyFrom(finalKey)
    byteKeyInc
  }

}
