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
package org.apache.kyuubi.engine.adbc.operation

import scala.collection.JavaConverters._

import org.apache.arrow.adbc.core.{AdbcConnection, AdbcException, AdbcStatusCode}
import org.apache.arrow.adbc.core.AdbcConnection.GetObjectsDepth
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.engine.adbc.schema._
import org.apache.kyuubi.engine.adbc.session.AdbcSessionImpl
import org.apache.kyuubi.operation.{ArrayFetchIterator, OperationState}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

/** [[AdbcMetadataSource]] over the driver's `getObjects`, `getTableTypes` and `getTableSchema`. */
class ConnectionMetadataSource(connection: AdbcConnection) extends AdbcMetadataSource {

  override def objects(
      depth: AdbcMetadataSource.Depth,
      catalog: String,
      schema: String,
      table: String,
      tableTypes: Array[String],
      column: String): Seq[ObjectCatalog] = {
    val d = depth match {
      case AdbcMetadataSource.Catalogs => GetObjectsDepth.CATALOGS
      case AdbcMetadataSource.Schemas => GetObjectsDepth.DB_SCHEMAS
      case AdbcMetadataSource.Tables => GetObjectsDepth.TABLES
      case AdbcMetadataSource.All => GetObjectsDepth.ALL
    }
    val reader = connection.getObjects(d, catalog, schema, table, tableTypes, column)
    try AdbcObjects.read(reader)
    finally reader.close()
  }

  override def tableTypes(): Seq[String] = {
    val reader = connection.getTableTypes()
    try {
      val root = reader.getVectorSchemaRoot
      val out = Seq.newBuilder[String]
      while (reader.loadNextBatch()) {
        val types = root.getVector(0)
        (0 until root.getRowCount).foreach { i =>
          if (!types.isNull(i)) out += types.getObject(i).toString
        }
      }
      out.result()
    } finally reader.close()
  }

  override def tableSchema(catalog: String, schema: String, table: String): Option[Schema] =
    try Option(connection.getTableSchema(catalog, schema, table))
    catch {
      case e: AdbcException if e.getStatus == AdbcStatusCode.NOT_IMPLEMENTED => None
    }
}

/**
 * Metadata operations are answered from the driver's `getObjects` and friends on the session's
 * metadata connection, never the query connection (a driver may refuse a second statement while a
 * result is open there). Everything is read and the driver's reader closed before the operation
 * finishes, so nothing native outlives `runInternal`.
 */
abstract class AdbcMetadataOperation(session: Session) extends AdbcOperation(session) with Logging {

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  protected def resultSchema: List[AdbcColumn]

  protected def rows(source: AdbcMetadataSource): Seq[Seq[Any]]

  protected def adbcSession: AdbcSessionImpl = session.asInstanceOf[AdbcSessionImpl]

  override protected def runInternal(): Unit = {
    setState(OperationState.RUNNING)
    try {
      schema = resultSchema
      val result = adbcSession.withMetadataConnection(c => rows(new ConnectionMetadataSource(c)))
      iter = new ArrayFetchIterator(result.toArray)
      setState(OperationState.FINISHED)
    } catch {
      case e: AdbcException if e.getStatus == AdbcStatusCode.NOT_IMPLEMENTED =>
        onError()(KyuubiSQLException.featureNotSupported(
          s"The ADBC driver does not support this metadata call: ${e.getMessage}"))
      case t: Throwable => onError()(t)
    }
  }
}

class GetCatalogs(session: Session) extends AdbcMetadataOperation(session) {
  override protected def resultSchema: List[AdbcColumn] = AdbcMetadataSchemas.Catalogs
  override protected def rows(source: AdbcMetadataSource): Seq[Seq[Any]] =
    AdbcMetadataQueries.catalogs(source)
}

class GetSchemas(session: Session, catalog: String, schemaPattern: String)
  extends AdbcMetadataOperation(session) {
  override protected def resultSchema: List[AdbcColumn] = AdbcMetadataSchemas.Schemas
  override protected def rows(source: AdbcMetadataSource): Seq[Seq[Any]] =
    AdbcMetadataQueries.schemas(source, catalog, schemaPattern)
}

class GetTableTypes(session: Session) extends AdbcMetadataOperation(session) {
  override protected def resultSchema: List[AdbcColumn] = AdbcMetadataSchemas.TableTypes
  override protected def rows(source: AdbcMetadataSource): Seq[Seq[Any]] =
    AdbcMetadataQueries.tableTypes(source)
}

class GetTables(
    session: Session,
    catalog: String,
    schemaPattern: String,
    tablePattern: String,
    tableTypes: java.util.List[String])
  extends AdbcMetadataOperation(session) {
  override protected def resultSchema: List[AdbcColumn] = AdbcMetadataSchemas.Tables
  override protected def rows(source: AdbcMetadataSource): Seq[Seq[Any]] =
    AdbcMetadataQueries.tables(
      source,
      catalog,
      schemaPattern,
      tablePattern,
      Option(tableTypes).map(_.asScala.toSeq).orNull)
}

class GetColumns(
    session: Session,
    catalog: String,
    schemaPattern: String,
    tablePattern: String,
    columnPattern: String)
  extends AdbcMetadataOperation(session) {
  override protected def resultSchema: List[AdbcColumn] = AdbcMetadataSchemas.Columns
  override protected def rows(source: AdbcMetadataSource): Seq[Seq[Any]] =
    AdbcMetadataQueries.columns(
      source,
      catalog,
      schemaPattern,
      tablePattern,
      columnPattern,
      (table, e) => warn(
        s"Could not read the schema of table ${table.name}; using the driver's column types: $e"))
}

/**
 * The current catalog and schema are connection state, and statements run on the query connection,
 * so these go there. They do not wait for a statement that is running on it: as with any JDBC
 * connection, the client must not set the current catalog while a statement of the same session is
 * still executing or being read.
 */
abstract class AdbcCurrentOperation(session: Session) extends AdbcOperation(session) with Logging {

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  protected def what: String

  protected def run(connection: AdbcConnection): Option[String]

  protected def resultSchema: List[AdbcColumn]

  override protected def runInternal(): Unit = {
    setState(OperationState.RUNNING)
    try {
      val value = session.asInstanceOf[AdbcSessionImpl].withQueryConnection(run)
      value match {
        case Some(v) =>
          schema = resultSchema
          iter = new ArrayFetchIterator(Array(Seq[Any](v)))
        case None =>
          setHasResultSet(false)
      }
      setState(OperationState.FINISHED)
    } catch {
      case e: AdbcException if e.getStatus == AdbcStatusCode.NOT_IMPLEMENTED =>
        onError()(KyuubiSQLException.featureNotSupported(
          s"The ADBC driver does not support $what: ${e.getMessage}"))
      case t: Throwable => onError()(t)
    }
  }
}

class GetCurrentCatalog(session: Session) extends AdbcCurrentOperation(session) {
  override protected def what = "getting the current catalog"
  override protected def resultSchema: List[AdbcColumn] = AdbcMetadataSchemas.CurrentCatalog
  override protected def run(c: AdbcConnection): Option[String] = Some(c.getCurrentCatalog)
}

class GetCurrentDatabase(session: Session) extends AdbcCurrentOperation(session) {
  override protected def what = "getting the current schema"
  override protected def resultSchema: List[AdbcColumn] = AdbcMetadataSchemas.CurrentDatabase
  override protected def run(c: AdbcConnection): Option[String] = Some(c.getCurrentDbSchema)
}

class SetCurrentCatalog(session: Session, catalog: String) extends AdbcCurrentOperation(session) {
  override protected def what = "setting the current catalog"
  override protected def resultSchema: List[AdbcColumn] = Nil
  override protected def run(c: AdbcConnection): Option[String] = {
    c.setCurrentCatalog(catalog)
    None
  }
}

class SetCurrentDatabase(session: Session, database: String)
  extends AdbcCurrentOperation(session) {
  override protected def what = "setting the current schema"
  override protected def resultSchema: List[AdbcColumn] = Nil
  override protected def run(c: AdbcConnection): Option[String] = {
    c.setCurrentDbSchema(database)
    None
  }
}
