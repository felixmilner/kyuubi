/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kyuubi.engine.adbc.session

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.util.control.NonFatal

import org.apache.arrow.adbc.core.{AdbcConnection, AdbcDatabase}
import org.apache.arrow.memory.BufferAllocator

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_ADBC_INITIALIZE_SQL
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_HANDLE_KEY
import org.apache.kyuubi.engine.adbc.util.AdbcDatabases
import org.apache.kyuubi.session.{AbstractSession, SessionHandle, SessionManager}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TGetInfoType, TGetInfoValue, TProtocolVersion}

/**
 * A session owns one ADBC database and two connections: `queryConnection` runs statements and keeps
 * session state (temp tables, transactions); `metadataConnection` is for metadata calls, because some
 * drivers (Quack) refuse a second statement on a connection that has a result open.
 */
class AdbcSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: SessionManager)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  override val handle: SessionHandle =
    conf.get(KYUUBI_SESSION_HANDLE_KEY).map(SessionHandle.fromUUID).getOrElse(SessionHandle())

  /**
   * Driver, URI, driver options and init SQL are read from the engine's launch conf only. That conf
   * is built by the Kyuubi server from its own settings and the launching session's; the server
   * ignores these keys when a client sets them (KyuubiConf.ADBC_ENGINE_LAUNCH_CONF, applied in
   * SessionManager), because the init SQL can use `${env:NAME}` and the engine inherits the
   * server's environment.
   */
  val engineConf: KyuubiConf = sessionManager.getConf

  /**
   * The engine conf with the client's session conf on top, for per-session behaviour settings such
   * as incremental collect. Never used for the driver, its options or the init SQL.
   */
  val sessionConf: KyuubiConf = {
    val c = engineConf.clone
    conf.foreach { case (k, v) => c.set(k, v) }
    c
  }

  private var allocator: BufferAllocator = _
  private var database: AdbcDatabase = _
  @volatile private[adbc] var queryConnection: AdbcConnection = _
  @volatile private[adbc] var metadataConnection: AdbcConnection = _

  private[adbc] def bufferAllocator: BufferAllocator = allocator

  // Native calls (executing, reading, closing a result) hold the read side; closing the connections
  // and the database takes the write side, so they are never closed under a call still in the driver.
  private val nativeGate = new ReentrantReadWriteLock()

  private[adbc] def withNativeAccess[T](body: => T): T = {
    nativeGate.readLock().lock()
    try body
    finally nativeGate.readLock().unlock()
  }

  override def open(): Unit = {
    info("Starting to open ADBC session.")
    try {
      allocator = AdbcDatabases.allocator.newChildAllocator(s"session-$handle", 0, Long.MaxValue)
      database = AdbcDatabases.open(engineConf, allocator)
      queryConnection = database.connect()
      metadataConnection = database.connect()
      AdbcDatabases.runStatements(queryConnection, engineConf.get(ENGINE_ADBC_INITIALIZE_SQL))
    } catch {
      case NonFatal(e) =>
        closeQuietly()
        throw KyuubiSQLException(s"Failed to open ADBC session: ${e.getMessage}", e)
    }
    super.open()
    info("The ADBC session is started.")
  }

  override def getInfo(infoType: TGetInfoType): TGetInfoValue = withAcquireRelease() {
    infoType match {
      case TGetInfoType.CLI_SERVER_NAME | TGetInfoType.CLI_DBMS_NAME =>
        TGetInfoValue.stringValue("ADBC")
      case TGetInfoType.CLI_DBMS_VER => TGetInfoValue.stringValue("1.0")
      case TGetInfoType.CLI_ODBC_KEYWORDS => TGetInfoValue.stringValue("Unimplemented")
      case _ => throw KyuubiSQLException(s"Unrecognized GetInfoType value: $infoType")
    }
  }

  override def close(): Unit = {
    // Operations first: each asks its driver to stop and releases its result. Then wait, bounded,
    // for any call still inside the driver before the connections and database are closed.
    super.close()
    if (nativeGate.writeLock().tryLock(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS)) {
      try closeQuietly()
      finally nativeGate.writeLock().unlock()
    } else {
      warn(s"A native ADBC call was still running $CLOSE_WAIT_SECONDS s after the session closed;" +
        " its connections are left open rather than closed under it.")
    }
  }

  private final val CLOSE_WAIT_SECONDS = 30L

  private def closeQuietly(): Unit = {
    Seq[() => Unit](
      () => if (queryConnection != null) queryConnection.close(),
      () => if (metadataConnection != null) metadataConnection.close(),
      () => if (database != null) database.close(),
      () => if (allocator != null) allocator.close()).foreach { close =>
      try close()
      catch { case NonFatal(e) => warn("Failed to close an ADBC session resource, ignored.", e) }
    }
    queryConnection = null
    metadataConnection = null
    database = null
    allocator = null
  }
}
