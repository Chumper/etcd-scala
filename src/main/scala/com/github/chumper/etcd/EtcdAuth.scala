package com.github.chumper.etcd

import authpb.auth.Permission
import com.github.chumper.grpc
import etcdserverpb.rpc.AuthGrpc.AuthStub
import etcdserverpb.rpc._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by n.plaschke on 03/01/2017.
  */
class EtcdAuth(private var stub: AuthStub)(implicit val ec: ExecutionContext) {

  def withToken(token: String): Unit = {
    this.stub = AuthGrpc.stub(stub.getChannel).withCallCredentials(grpc.EtcdTokenCredentials(token))
  }

  def authenticate(username: String, pass: String): Future[String] = {
    stub.authenticate(AuthenticateRequest(
      name = username,
      password = pass
    )).map { resp =>
      resp.token
    }
  }

  def enable(): Future[AuthEnableResponse] = {
    stub.authEnable(AuthEnableRequest())
  }

  def disable(): Future[AuthDisableResponse] = {
    stub.authDisable(AuthDisableRequest())
  }

  def addUser(name: String, password: String): Future[AuthUserAddResponse] = {
    stub.userAdd(AuthUserAddRequest(
      name = name,
      password = password
    ))
  }

  def getUser(name: String): Future[AuthUserGetResponse] = {
    stub.userGet(AuthUserGetRequest(
      name = name
    ))
  }

  def allUsers(): Future[AuthUserListResponse] = {
    stub.userList(AuthUserListRequest(
    ))
  }

  def deleteUser(name: String): Future[AuthUserDeleteResponse] = {
    stub.userDelete(AuthUserDeleteRequest(
      name = name
    ))
  }

  def changePassword(name: String, password: String): Future[AuthUserChangePasswordResponse] = {
    stub.userChangePassword(AuthUserChangePasswordRequest(
      name = name,
      password = password
    ))
  }

  def grantRole(username: String, role: String): Future[AuthUserGrantRoleResponse] = {
    stub.userGrantRole(AuthUserGrantRoleRequest(
      user = username,
      role = role
    ))
  }

  def revokeRole(username: String, role: String): Future[AuthUserRevokeRoleResponse] = {
    stub.userRevokeRole(AuthUserRevokeRoleRequest(
      name = username,
      role = role
    ))
  }

  def addRole(name: String): Future[AuthRoleAddResponse] = {
    stub.roleAdd(AuthRoleAddRequest(
      name = name
    ))
  }

  def getRole(name: String): Future[AuthRoleGetResponse] = {
    stub.roleGet(AuthRoleGetRequest(
      role = name
    ))
  }

  def allRoles(): Future[AuthRoleListResponse] = {
    stub.roleList(AuthRoleListRequest())
  }

  def deleteRole(name: String): Future[AuthRoleDeleteResponse] = {
    stub.roleDelete(AuthRoleDeleteRequest(
      role = name
    ))
  }

  def grantPermission(name: String, permission: Option[Permission]): Future[AuthRoleGrantPermissionResponse] = {
    stub.roleGrantPermission(AuthRoleGrantPermissionRequest(
      name = name,
      perm = permission
    ))
  }

  def revokePermission(name: String, key: String): Future[AuthRoleRevokePermissionResponse] = {
    stub.roleRevokePermission(AuthRoleRevokePermissionRequest(
      role = name,
      key = key
    ))
  }
}
