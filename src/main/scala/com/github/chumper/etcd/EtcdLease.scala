package com.github.chumper.etcd

import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import com.github.chumper.grpc
import etcdserverpb.rpc.LeaseGrpc.LeaseStub
import etcdserverpb.rpc._
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * Created by n.plaschke on 03/01/2017.
  */
class EtcdLease(private var stub: LeaseStub)(implicit val ec: ExecutionContext) {

  def withToken(token: String): Unit = {
    this.stub = LeaseGrpc.stub(stub.getChannel).withCallCredentials(grpc.EtcdTokenCredentials(token))
  }

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
