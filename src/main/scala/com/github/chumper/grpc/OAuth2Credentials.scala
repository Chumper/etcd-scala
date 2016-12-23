package com.github.chumper.grpc

import java.util.concurrent.Executor

import io.grpc.CallCredentials.MetadataApplier
import io.grpc.{Attributes, CallCredentials, Metadata, MethodDescriptor}

class OAuth2Credentials(token: String) extends CallCredentials {
  override def applyRequestMetadata(method: MethodDescriptor[_, _], attrs: Attributes, appExecutor: Executor, applier: MetadataApplier): Unit = {
    val metaData = new Metadata()
    metaData.put(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER), s"Bearer $token")
    applier.apply(metaData)
  }
}

object OAuth2Credentials {
  def apply(token: String): OAuth2Credentials = new OAuth2Credentials(token)
}