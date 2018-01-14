package com.github.chumper.etcd

import etcdserverpb.rpc._
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.language.postfixOps

/**
  * Main trait for access to all etcd operations
  */
class Etcd(address: String, port: Int, plainText: Boolean = true, token: Option[String] = None)(implicit val ec: ExecutionContext) {

  private val builder: ManagedChannelBuilder[_ <: ManagedChannelBuilder[_]] = ManagedChannelBuilder.forAddress(address, port)
  if (plainText) {
    builder.usePlaintext(true)
  }

  private val channel: ManagedChannel = builder.build()

  val kv: EtcdKv = new EtcdKv(KVGrpc.stub(channel))
  val lease: EtcdLease = new EtcdLease(LeaseGrpc.stub(channel))
  val watch: EtcdWatch = new EtcdWatch(WatchGrpc.stub(channel))
  val auth: EtcdAuth = new EtcdAuth(AuthGrpc.stub(channel))

  /**
    * If you need to login with a username and password on the etcd instance. It will get the request token and uses
    * that on all stubs
    * @param username The username to login
    * @param password The password to login
    * @return The etcd instance with authentication enabled if possible
    */
  def withAuth(username: String, password: String): Etcd = {
    val t = Await.result(auth.authenticate(username, password), 3 seconds)
    withToken(t)
  }

  /**
    * If you already have a token for the etcd instance you can directly use the token to update all stubs with it.
    * @param token The token retrieved from the login process
    * @return The etcd instance with authentication enabled if possible
    */
  def withToken(token: String): Etcd = {
    kv.withToken(token)
    lease.withToken(token)
    watch.withToken(token)
    auth.withToken(token)
    this
  }
}

object Etcd {
  def apply(address: String = "localhost", port: Int = 2379, plainText: Boolean = true, token: Option[String] = None)(implicit ec: ExecutionContext) = new Etcd(address, port, plainText, token)
}