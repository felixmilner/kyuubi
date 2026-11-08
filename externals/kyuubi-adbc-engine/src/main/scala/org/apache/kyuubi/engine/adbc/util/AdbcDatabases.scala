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
package org.apache.kyuubi.engine.adbc.util

import scala.util.control.NonFatal

import org.apache.arrow.adbc.core.{AdbcConnection, AdbcDatabase}
import org.apache.arrow.adbc.driver.jni.JniDriver
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_ADBC_DRIVER, ENGINE_ADBC_URI}

object AdbcDatabases extends Logging {

  final val OPTION_PREFIX = "kyuubi.engine.adbc.option."

  /** One Arrow root allocator for the engine JVM; sessions take child allocators from it. */
  lazy val allocator: BufferAllocator = new RootAllocator()

  def closeAllocator(): Unit = {
    try {
      allocator.close()
    } catch {
      case NonFatal(e) => warn("Failed to close the Arrow allocator, ignored.", e)
    }
  }

  /**
   * Opens an ADBC database from the engine's launch configuration. Driver, URI and driver options
   * come only from the configuration the server launched the engine with, never from the client's
   * session configuration, so a client cannot point the engine at another driver.
   */
  def open(conf: KyuubiConf, sessionAllocator: BufferAllocator): AdbcDatabase = {
    val driver = conf.get(ENGINE_ADBC_DRIVER).getOrElse(
      throw KyuubiSQLException(s"${ENGINE_ADBC_DRIVER.key} is not set"))
    val builder = new JniDriver(sessionAllocator).load().driver(driver)
    conf.get(ENGINE_ADBC_URI).foreach(uri => builder.uri(uri))
    conf.getAll.filter(_._1.startsWith(OPTION_PREFIX)).foreach { case (k, v) =>
      builder.param(k.substring(OPTION_PREFIX.length), v)
    }
    builder.open()
  }

  private val ENV_PLACEHOLDER = """\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}""".r

  /**
   * Fills `${env:NAME}` placeholders from the engine's environment, so initialization SQL (for
   * example `CREATE SECRET`) can use a credential the server passed to the engine as an environment
   * variable without it appearing in the configuration, the command line or the logs. Returns the
   * expanded SQL and the values that were substituted.
   */
  private[util] def expandEnv(
      sql: String,
      env: String => Option[String] = n => Option(System.getenv(n))): (String, Seq[String]) = {
    val values = scala.collection.mutable.ArrayBuffer.empty[String]
    val expanded = ENV_PLACEHOLDER.replaceAllIn(
      sql,
      m => {
        val value = env(m.group(1)).getOrElse(
          throw KyuubiSQLException(
            s"Environment variable ${m.group(1)} used in the initialization sql is not set"))
        values += value
        scala.util.matching.Regex.quoteReplacement(value)
      })
    (expanded, values.toSeq)
  }

  /**
   * Runs each statement on the connection and drains its result. `${env:NAME}` placeholders are
   * expanded just before execution; only the unexpanded statement is logged or reported, and any
   * expanded value is removed from error messages (drivers often quote the failing SQL).
   */
  def runStatements(connection: AdbcConnection, statements: Seq[String]): Unit = {
    statements.map(_.trim).filter(_.nonEmpty).foreach { template =>
      debug(s"Execute initialization sql: $template")
      val (sql, secrets) = expandEnv(template)
      val stmt = connection.createStatement()
      try {
        stmt.setSqlQuery(sql)
        val result = stmt.executeQuery()
        try {
          val reader = result.getReader
          while (reader.loadNextBatch()) {}
        } finally {
          result.close()
        }
      } catch {
        case NonFatal(e) if secrets.nonEmpty =>
          // no cause: its message and stack trace would carry the expanded statement
          val redacted = secrets.filter(_.nonEmpty).foldLeft(String.valueOf(e.getMessage)) {
            (msg, secret) => msg.replace(secret, "<redacted>")
          }
          throw KyuubiSQLException(s"Initialization sql failed: $template: $redacted")
      } finally {
        stmt.close()
      }
    }
  }
}
