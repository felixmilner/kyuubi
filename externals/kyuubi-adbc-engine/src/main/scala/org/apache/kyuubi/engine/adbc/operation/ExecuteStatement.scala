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

import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.arrow.adbc.core.{AdbcException, AdbcStatement, AdbcStatusCode}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowReader

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.engine.adbc.operation.ExecuteStatement.ArrowMaxRowsPerBatch
import org.apache.kyuubi.engine.adbc.schema.{AdbcArrowEncoder, AdbcColumn, AdbcSchema}
import org.apache.kyuubi.engine.adbc.session.AdbcSessionImpl
import org.apache.kyuubi.operation.{ArrayFetchIterator, FetchOrientation, IterableFetchIterator, OperationState}
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

class ExecuteStatement(
    session: Session,
    override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long,
    incrementalCollect: Boolean,
    useArrow: Boolean = false,
    arrowTimestampAsString: Boolean = false)
  extends AdbcOperation(session) with Logging {

  override protected def arrowFormat: Boolean = useArrow
  override protected def timestampAsString: Boolean = arrowTimestampAsString

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  @volatile private var adbcStatement: AdbcStatement = _
  @volatile private var queryResult: AdbcStatement.QueryResult = _
  // Arrow format only: the converted batches are made inside the native section from the driver's
  // vectors, with an allocator of its own that is released together with the result.
  @volatile private var encoder: AdbcArrowEncoder = _
  @volatile private var encoderAllocator: BufferAllocator = _
  // set once the operation is cancelled, timed out or closed
  @volatile private var abandoned = false

  // Held while any thread is inside the driver's executeQuery() or reader (loadNextBatch()), and
  // while the result and statement are closed. Closing them natively while another thread is still
  // reading aborts the process (found in M1: DuckDB, "Attempted to dereference shared_ptr that is
  // NULL"), so cancel() only asks the driver to stop (stmt.cancel() is made without this lock) and
  // the reading thread closes the resources once its read has returned.
  private val nativeLock = new ReentrantLock()

  private def adbcSession: AdbcSessionImpl = session.asInstanceOf[AdbcSessionImpl]

  // the session's gate keeps its connections and database open while this call is in the driver
  private def native[T](body: => T): T = {
    nativeLock.lock()
    try adbcSession.withNativeAccess(body)
    finally nativeLock.unlock()
  }

  /** Closes the result and statement now if no thread is reading; otherwise the reader does it. */
  private def closeIfIdle(): Unit = {
    if (nativeLock.tryLock()) {
      try adbcSession.withNativeAccess(closeResources())
      finally nativeLock.unlock()
    }
  }

  override protected def runInternal(): Unit = {
    addTimeoutMonitor(queryTimeout)
    if (shouldRunAsync) {
      val asyncOperation = new Runnable {
        override def run(): Unit = {
          executeStatement()
        }
      }
      try {
        val backgroundHandle = session.sessionManager.submitBackgroundOperation(asyncOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          val ke =
            KyuubiSQLException("Error submitting query in background, query rejected", rejected)
          setOperationException(ke)
          shutdownTimeoutMonitor()
          throw ke
      }
    } else {
      executeStatement()
    }
  }

  private def executeStatement(): Unit = {
    setState(OperationState.RUNNING)
    try {
      val connection = session.asInstanceOf[AdbcSessionImpl].queryConnection
      val result = native {
        val stmt = connection.createStatement()
        adbcStatement = stmt
        stmt.setSqlQuery(statement)
        val r = stmt.executeQuery()
        queryResult = r
        r
      }
      if (!abandoned) {
        val reader = result.getReader
        val root = reader.getVectorSchemaRoot
        schema = AdbcSchema.columns(root.getSchema, reader)
        if (useArrow) {
          native {
            if (!abandoned) {
              encoderAllocator = adbcSession.bufferAllocator
                .newChildAllocator(s"arrow-result-$statementId", 0, Long.MaxValue)
              encoder = new AdbcArrowEncoder(
                schema,
                root.getSchema,
                arrowTimestampAsString,
                encoderAllocator,
                ArrowMaxRowsPerBatch)
            }
          }
          val batches = convertedBatches(reader, root)(r => encoder.encode(r))
          if (incrementalCollect) {
            info("Execute in incremental collect mode, Arrow format")
            arrowIter = new IterableFetchIterator(new Iterable[Array[Byte]] {
              override def iterator: Iterator[Array[Byte]] = batches
            })
          } else {
            val all = ArrayBuffer.empty[Array[Byte]]
            batches.foreach(all += _)
            arrowIter = new ArrayFetchIterator(all.toArray)
          }
        } else if (incrementalCollect) {
          info("Execute in incremental collect mode")
          val rows = rowIterator(reader, root, schema)
          iter = new IterableFetchIterator(new Iterable[Seq[Any]] {
            override def iterator: Iterator[Seq[Any]] = rows
          })
        } else {
          val all = ArrayBuffer.empty[Seq[Any]]
          rowIterator(reader, root, schema).foreach(all += _)
          iter = new ArrayFetchIterator(all.toArray)
        }
        if (!abandoned) setState(OperationState.FINISHED)
      }
    } catch {
      onError(true)
    } finally {
      // The result stays open only for an incremental fetch of a finished operation; every other
      // outcome (error, cancel, timeout, full collect) releases it here, from the reading thread.
      val keepOpen = incrementalCollect && state == OperationState.FINISHED && !abandoned
      if (!keepOpen) native { closeResources() }
      shutdownTimeoutMonitor()
    }
  }

  /**
   * Rows of every batch the reader produces; values are copied out of the reused vectors.
   *
   * Everything that touches the driver's vectors (loading a batch and reading a row) happens inside
   * the native section, and `abandoned` is checked there: cancel() can close the result as soon as
   * no thread is in a native section, so a check made before taking the lock proves nothing, and
   * the vectors are freed memory once the result is closed.
   */
  private def rowIterator(
      reader: ArrowReader,
      root: VectorSchemaRoot,
      columns: List[AdbcColumn]): Iterator[Seq[Any]] = new Iterator[Seq[Any]] {
    private val vectors = root.getFieldVectors
    private var index = 0
    private var rowCount = 0
    private var finished = false

    private def advance(): Unit = {
      while (!finished && index >= rowCount) {
        val loaded = native {
          if (abandoned) false
          else if (reader.loadNextBatch()) {
            rowCount = root.getRowCount
            index = 0
            true
          } else false
        }
        if (!loaded) finished = true
      }
    }

    override def hasNext: Boolean = {
      advance()
      !finished
    }

    override def next(): Seq[Any] = {
      if (!hasNext) throw new NoSuchElementException("no more rows")
      val i = index
      index += 1
      native {
        if (abandoned) throw KyuubiSQLException("The operation was cancelled or closed")
        columns.zipWithIndex.map { case (c, j) => c.read(vectors.get(j), i) }
      }
    }
  }

  /**
   * Batches the reader produces, converted by `convert` inside the native section (the conversion
   * copies, so the result holds no driver memory).
   */
  private def convertedBatches[T](reader: ArrowReader, root: VectorSchemaRoot)(
      convert: VectorSchemaRoot => Seq[T]): Iterator[T] = new Iterator[T] {
    private var pending: Iterator[T] = Iterator.empty
    private var finished = false

    private def advance(): Unit = {
      while (!finished && !pending.hasNext) {
        val next = native {
          if (abandoned) None
          else if (reader.loadNextBatch()) Some(convert(root))
          else None
        }
        next match {
          case Some(batches) => pending = batches.iterator
          case None => finished = true
        }
      }
    }

    override def hasNext: Boolean = {
      advance()
      !finished
    }

    override def next(): T = {
      if (!hasNext) throw new NoSuchElementException("no more batches")
      pending.next()
    }
  }

  override def validateFetchOrientation(order: FetchOrientation): Unit = {
    if (incrementalCollect && order != FetchOrientation.FETCH_NEXT) {
      throw KyuubiSQLException(s"The fetch type $order is not supported" +
        " of incremental collect mode.")
    }
    super.validateFetchOrientation(order)
  }

  /**
   * Asks the driver to stop. Only some drivers implement it (DuckDB does; DataFusion and the Quack
   * driver return NOT_IMPLEMENTED), the others stop when the result is closed or, if the query is
   * still executing, when it returns.
   */
  private def cancelDriver(): Unit = {
    val stmt = adbcStatement
    if (stmt != null) {
      try {
        stmt.cancel()
      } catch {
        case e: AdbcException if e.getStatus == AdbcStatusCode.NOT_IMPLEMENTED =>
          debug(s"The ADBC driver does not implement cancel for $statementId")
        case NonFatal(e) =>
          warn(s"Failed to cancel the ADBC statement of $statementId", e)
      }
    }
  }

  private def closeResources(): Unit = {
    val result = queryResult
    val stmt = adbcStatement
    val enc = encoder
    val encAllocator = encoderAllocator
    queryResult = null
    adbcStatement = null
    encoder = null
    encoderAllocator = null
    // the encoder's buffers are the engine's own, released before the driver's result
    Seq[() => Unit](
      () => if (enc != null) enc.close(),
      () => if (encAllocator != null) encAllocator.close(),
      () => if (result != null) result.close(),
      () => if (stmt != null) stmt.close()).foreach { close =>
      try close()
      catch { case NonFatal(e) => warn(s"Failed to close an ADBC resource of $statementId", e) }
    }
  }

  override def cancel(): Unit = withLockRequired {
    if (!isTerminalState(state)) {
      setState(OperationState.CANCELED)
      abandoned = true
      cancelDriver()
      // if a thread is reading, it closes the result when the interrupted read returns
      closeIfIdle()
    }
  }

  override def cleanup(targetState: OperationState): Unit = withLockRequired {
    try {
      super.cleanup(targetState)
    } finally {
      abandoned = true
      // CANCELED, TIMEOUT and CLOSED can all arrive while a query is still running
      cancelDriver()
      closeIfIdle()
    }
  }
}

object ExecuteStatement {
  // Upper bound of rows in one Arrow batch (one Thrift fetch). DuckDB's batches can be far larger;
  // the encoder slices them. A row-count cap does not bound the bytes of wide or string-heavy rows:
  // see the M2 notes in DESIGN.md.
  val ArrowMaxRowsPerBatch = 8192
}
