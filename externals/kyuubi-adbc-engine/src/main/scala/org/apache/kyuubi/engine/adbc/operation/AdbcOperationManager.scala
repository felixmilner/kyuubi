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

import java.util

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf.ENGINE_ADBC_OPERATION_INCREMENTAL_COLLECT
import org.apache.kyuubi.operation.{Operation, OperationManager}
import org.apache.kyuubi.session.Session

/**
 * M1 runs statements only. The metadata operations (catalogs, schemas, tables, columns, ...) are
 * added in M2 on top of the driver's getObjects / getTableSchema.
 */
class AdbcOperationManager extends OperationManager("AdbcOperationManager") {

  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
    val engineConf = session.sessionManager.getConf
    val incrementalCollect = confOverlay
      .get(ENGINE_ADBC_OPERATION_INCREMENTAL_COLLECT.key)
      .map(_.toBoolean)
      .getOrElse(engineConf.get(ENGINE_ADBC_OPERATION_INCREMENTAL_COLLECT))
    addOperation(
      new ExecuteStatement(session, statement, runAsync, queryTimeout, incrementalCollect))
  }

  private def notSupported(what: String): Nothing =
    throw KyuubiSQLException.featureNotSupported(s"$what is not supported by the ADBC engine yet")

  override def newSetCurrentCatalogOperation(session: Session, catalog: String): Operation =
    notSupported("Setting the current catalog")

  override def newGetCurrentCatalogOperation(session: Session): Operation =
    notSupported("Getting the current catalog")

  override def newSetCurrentDatabaseOperation(session: Session, database: String): Operation =
    notSupported("Setting the current database")

  override def newGetCurrentDatabaseOperation(session: Session): Operation =
    notSupported("Getting the current database")

  override def newGetTypeInfoOperation(session: Session): Operation =
    notSupported("GetTypeInfo")

  override def newGetCatalogsOperation(session: Session): Operation =
    notSupported("GetCatalogs")

  override def newGetSchemasOperation(
      session: Session,
      catalog: String,
      schema: String): Operation =
    notSupported("GetSchemas")

  override def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: util.List[String]): Operation =
    notSupported("GetTables")

  override def newGetTableTypesOperation(session: Session): Operation =
    notSupported("GetTableTypes")

  override def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation =
    notSupported("GetColumns")

  override def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation =
    notSupported("GetFunctions")

  override def newGetPrimaryKeysOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String): Operation =
    notSupported("GetPrimaryKeys")

  override def newGetCrossReferenceOperation(
      session: Session,
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): Operation =
    notSupported("GetCrossReference")

  override def getQueryId(operation: Operation): String = {
    throw KyuubiSQLException.featureNotSupported()
  }
}
