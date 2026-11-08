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

package org.apache.kyuubi.engine.adbc

import java.io.File
import java.nio.file.Paths

import scala.collection.mutable

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.{Logging, SCALA_COMPILE_VERSION, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_ADBC_DRIVER, ENGINE_ADBC_EXTRA_CLASSPATH, ENGINE_ADBC_JAVA_OPTIONS, ENGINE_ADBC_MEMORY}
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.{EngineType, ProcBuilder}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.command.CommandLineUtils._

/**
 * Launches the ADBC engine, a JVM per user that opens an ADBC driver (chosen by
 * `kyuubi.engine.adbc.driver`) through the ADBC JNI driver manager. Local deploy mode only.
 */
class AdbcProcessBuilder(
    override val proxyUser: String,
    override val doAsEnabled: Boolean,
    override val conf: KyuubiConf,
    val engineRefId: String,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder with Logging {

  @VisibleForTesting
  def this(proxyUser: String, doAsEnabled: Boolean, conf: KyuubiConf) {
    this(proxyUser, doAsEnabled, conf, "")
  }

  override def shortName: String = "adbc"

  override protected def module: String = "kyuubi-adbc-engine"

  override protected def mainClass: String = "org.apache.kyuubi.engine.adbc.AdbcSQLEngine"

  override protected val commands: Iterable[String] = {
    require(
      conf.get(ENGINE_ADBC_DRIVER).nonEmpty,
      s"ADBC driver can not be null! Please set ${ENGINE_ADBC_DRIVER.key}")
    val buffer = new mutable.ListBuffer[String]()
    buffer += executable

    val memory = conf.get(ENGINE_ADBC_MEMORY)
    buffer += s"-Xmx$memory"

    // Arrow's memory and C-data code need java.nio access on JDK 17+
    buffer += "--add-opens=java.base/java.nio=ALL-UNNAMED"

    val javaOptions = conf.get(ENGINE_ADBC_JAVA_OPTIONS).filter(StringUtils.isNotBlank(_))
    if (javaOptions.isDefined) {
      buffer ++= parseOptionString(javaOptions.get)
    }
    val classpathEntries = new mutable.LinkedHashSet[String]
    mainResource.foreach(classpathEntries.add)
    mainResource.foreach { path =>
      val parent = Paths.get(path).getParent
      if (Utils.isTesting) {
        // add dev classpath
        val adbcDeps = parent
          .resolve(s"scala-$SCALA_COMPILE_VERSION")
          .resolve("jars")
        classpathEntries.add(s"$adbcDeps${File.separator}*")
      } else {
        // add prod classpath
        classpathEntries.add(s"$parent${File.separator}*")
      }
    }

    val extraCp = conf.get(ENGINE_ADBC_EXTRA_CLASSPATH)
    extraCp.foreach(classpathEntries.add)
    buffer ++= genClasspathOption(classpathEntries)

    buffer += mainClass

    buffer ++= confKeyValue(KYUUBI_SESSION_USER_KEY, proxyUser)

    buffer ++= confKeyValues(conf.getEngineConf(EngineType.ADBC))

    buffer
  }

  override def toString: String = {
    if (commands == null) {
      super.toString
    } else {
      redactConfValues(Utils.redactCommandLineArgs(conf, commands), Set.empty[String]).map {
        case arg if arg.startsWith("-") => s"\\\n\t$arg"
        case arg => arg
      }.mkString(" ")
    }
  }
}
