package com.github.chumper.grpc

import java.util.concurrent.Executor

import io.grpc.CallCredentials.MetadataApplier
import io.grpc.{Attributes, CallCredentials, Metadata, MethodDescriptor}

class EtcdTokenCredentials(token: String) extends CallCredentials {
  override def applyRequestMetadata(method: MethodDescriptor[_, _], attrs: Attributes, appExecutor: Executor, applier: MetadataApplier): Unit = {
    val metaData = new Metadata()
    metaData.put(Metadata.Key.of("token", Metadata.ASCII_STRING_MARSHALLER), s"$token")
    applier.apply(metaData)
  }

  // Implement this, because we are using an experimental API, jadda jadda.
  override def thisUsesUnstableApi(): Unit = {}
}

object EtcdTokenCredentials {
  def apply(token: String): EtcdTokenCredentials = new EtcdTokenCredentials(token)
}