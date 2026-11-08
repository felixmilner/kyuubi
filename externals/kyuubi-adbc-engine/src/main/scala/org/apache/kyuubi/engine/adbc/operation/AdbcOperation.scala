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

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.engine.adbc.schema.{AdbcArrowTRowSet, AdbcColumn, AdbcSchema, AdbcTRowSetGenerator}
import org.apache.kyuubi.operation.{AbstractOperation, FetchIterator, OperationState}
import org.apache.kyuubi.operation.FetchOrientation.{FETCH_FIRST, FETCH_NEXT, FETCH_PRIOR, FetchOrientation}
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TFetchResultsResp, TGetResultSetMetadataResp, TRowSet}

abstract class AdbcOperation(session: Session) extends AbstractOperation(session) {

  protected var schema: List[AdbcColumn] = _

  // Rows (Thrift result format) or serialized Arrow record batches (Arrow format); only the one
  // that matches `arrowFormat` is set.
  protected var iter: FetchIterator[Seq[Any]] = _
  protected var arrowIter: FetchIterator[Array[Byte]] = _

  /** Whether results go out as Arrow record batches, for the kyuubi-hive-jdbc Arrow client. */
  protected def arrowFormat: Boolean = false

  protected def timestampAsString: Boolean = false

  def validateFetchOrientation(order: FetchOrientation): Unit =
    validateDefaultFetchOrientation(order)

  override def getNextRowSetInternal(
      order: FetchOrientation,
      rowSetSize: Int): TFetchResultsResp = {
    validateFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    val resultRowSet = if (arrowFormat) {
      // one serialized batch per fetch; the row set size does not apply (as in the Spark engine)
      order match {
        case FETCH_NEXT => arrowIter.fetchNext()
        case FETCH_PRIOR => arrowIter.fetchPrior(rowSetSize)
        case FETCH_FIRST => arrowIter.fetchAbsolute(0)
      }
      val rowSet =
        if (arrowIter.hasNext) AdbcArrowTRowSet(arrowIter.next()) else AdbcArrowTRowSet.empty
      rowSet.setStartRowOffset(arrowIter.getPosition)
      rowSet
    } else {
      order match {
        case FETCH_NEXT =>
          iter.fetchNext()
        case FETCH_PRIOR =>
          iter.fetchPrior(rowSetSize)
        case FETCH_FIRST =>
          iter.fetchAbsolute(0)
      }
      val taken = iter.take(rowSetSize)
      val rowSet = toTRowSet(taken)
      rowSet.setStartRowOffset(iter.getPosition)
      rowSet
    }
    val resp = new TFetchResultsResp(OK_STATUS)
    resp.setResults(resultRowSet)
    resp.setHasMoreRows(false)
    resp
  }

  override def close(): Unit = {
    cleanup(OperationState.CLOSED)
  }

  override def cancel(): Unit = {
    cleanup(OperationState.CANCELED)
  }

  protected def onError(cancel: Boolean = false): PartialFunction[Throwable, Unit] = {
    // Throwable rather than Exception: `java.lang.NoClassDefFoundError` and native-driver
    // errors surfaced as Errors could be thrown.
    case e: Throwable =>
      withLockRequired {
        val errMsg = Utils.stringifyException(e)
        if (state == OperationState.TIMEOUT) {
          val ke = KyuubiSQLException(s"Timeout operating $opType: $errMsg")
          setOperationException(ke)
          throw ke
        } else if (isTerminalState(state)) {
          setOperationException(KyuubiSQLException(errMsg))
          warn(s"Ignore exception in terminal state with $statementId: $errMsg")
        } else {
          error(s"Error operating $opType: $errMsg", e)
          val ke = KyuubiSQLException(s"Error operating $opType: $errMsg", e)
          setOperationException(ke)
          setState(OperationState.ERROR)
          throw ke
        }
      }
  }

  override protected def beforeRun(): Unit = {
    setState(OperationState.PENDING)
    setHasResultSet(true)
  }

  override protected def afterRun(): Unit = {}

  protected def toTRowSet(taken: Iterator[Seq[Any]]): TRowSet =
    AdbcTRowSetGenerator.toTRowSet(taken.toSeq, schema, getProtocolVersion)

  override def getResultSetMetadata: TGetResultSetMetadataResp = {
    val resp = new TGetResultSetMetadataResp
    resp.setSchema(AdbcSchema.toTTableSchema(schema))
    // The JDBC client picks its decoder from these hints, not from what it asked for.
    resp.setStatus(okStatusWithHints(Seq(
      s"__kyuubi_operation_result_format__=${if (arrowFormat) "arrow" else "thrift"}",
      s"__kyuubi_operation_result_arrow_timestampAsString__=$timestampAsString")))
    resp
  }

  override def shouldRunAsync: Boolean = false
}
