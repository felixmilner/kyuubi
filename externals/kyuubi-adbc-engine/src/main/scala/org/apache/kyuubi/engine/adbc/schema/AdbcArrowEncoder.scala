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
package org.apache.kyuubi.engine.adbc.schema

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.util.{ArrayList => JArrayList}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.{ArrowStreamWriter, WriteChannel}
import org.apache.arrow.vector.ipc.message.{IpcOption, MessageSerializer}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.util.ThriftUtils

/**
 * The Arrow types the Kyuubi JDBC client can decode, per Thrift type id.
 *
 * The client does not read a schema from the Arrow bytes. It builds one from the Thrift
 * `TTableSchema` (`ArrowUtils.toArrowType` in kyuubi-hive-jdbc) and loads each bare record batch
 * into it, so a batch whose buffers do not match that type is read as garbage or fails. The
 * driver's own batches therefore cannot be forwarded as they are: this table is the contract, and
 * [[AdbcArrowEncoder]] rewrites every batch to it. The client always sees complex types as text.
 */
object AdbcArrowTypes {

  /** The time zone advertised for timestamp columns (`session.timeZone` qualifier). */
  val TimestampZone = "UTC"

  val TimeZoneQualifier = "session.timeZone"

  /**
   * The Arrow type of a column as the client rebuilds it. With `timestampAsString` the client
   * rebuilds timestamps as strings.
   */
  def clientType(column: AdbcColumn, timestampAsString: Boolean): ArrowType = column.tTypeId match {
    case TTypeId.BOOLEAN_TYPE => ArrowType.Bool.INSTANCE
    case TTypeId.SMALLINT_TYPE => new ArrowType.Int(16, true)
    case TTypeId.INT_TYPE => new ArrowType.Int(32, true)
    case TTypeId.BIGINT_TYPE => new ArrowType.Int(64, true)
    case TTypeId.FLOAT_TYPE => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case TTypeId.DOUBLE_TYPE => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case TTypeId.DECIMAL_TYPE => ArrowType.Decimal.createDecimal(column.precision, column.scale, null)
    case TTypeId.DATE_TYPE => new ArrowType.Date(DateUnit.DAY)
    case TTypeId.TIMESTAMP_TYPE =>
      if (timestampAsString) ArrowType.Utf8.INSTANCE
      else new ArrowType.Timestamp(TimeUnit.MICROSECOND, TimestampZone)
    case _ => ArrowType.Utf8.INSTANCE
  }

  def clientSchema(columns: Seq[AdbcColumn], timestampAsString: Boolean): Schema = {
    val fields = new JArrayList[Field](columns.size)
    columns.foreach { c =>
      fields.add(new Field(
        c.name,
        new FieldType(true, clientType(c, timestampAsString), null),
        java.util.Collections.emptyList[Field]()))
    }
    new Schema(fields)
  }
}

/**
 * Rewrites the driver's record batches to the client's Arrow types and serializes them as the bare
 * record-batch messages the client reads (one per Thrift fetch), the same bytes the Spark engine
 * sends. Conversions are copies, so the output never refers to driver memory; call
 * [[encode]] while holding the native lock so the driver cannot free its buffers mid-copy.
 *
 * Not thread-safe: it owns one target root that every call reuses.
 */
class AdbcArrowEncoder(
    columns: List[AdbcColumn],
    sourceSchema: Schema,
    timestampAsString: Boolean,
    allocator: BufferAllocator,
    maxRowsPerBatch: Int) extends AutoCloseable {

  import AdbcArrowEncoder._

  require(maxRowsPerBatch > 0, "maxRowsPerBatch must be positive")
  require(columns.size == sourceSchema.getFields.size, "column count does not match the schema")

  val targetSchema: Schema = AdbcArrowTypes.clientSchema(columns, timestampAsString)
  private val target: VectorSchemaRoot = VectorSchemaRoot.create(targetSchema, allocator)

  private val writers: Array[Writer] =
    columns.zip(sourceSchema.getFields.toArray(new Array[Field](0))).map { case (c, f) =>
      writer(c, f.getType, timestampAsString)
    }.toArray

  /** Serialized batches of at most `maxRowsPerBatch` rows for everything `source` holds now. */
  def encode(source: VectorSchemaRoot): Seq[Array[Byte]] = {
    val rows = source.getRowCount
    val vectors = source.getFieldVectors
    val out = Vector.newBuilder[Array[Byte]]
    var from = 0
    while (from < rows) {
      val count = math.min(maxRowsPerBatch, rows - from)
      target.clear()
      target.allocateNew()
      var j = 0
      while (j < writers.length) {
        val src = vectors.get(j)
        val dst = target.getVector(j)
        val write = writers(j)
        var i = 0
        while (i < count) {
          if (!src.isNull(from + i)) write(src, dst, from + i, i)
          i += 1
        }
        dst.setValueCount(count)
        j += 1
      }
      target.setRowCount(count)
      out += serialize(target)
      from += count
    }
    out.result()
  }

  override def close(): Unit = target.close()
}

object AdbcArrowEncoder {

  /** Copies (and converts) one non-null value: source vector, target vector, source row, target row. */
  private type Writer = (FieldVector, FieldVector, Int, Int) => Unit

  /** Spark's wire form: the record-batch message, then the IPC end-of-stream marker. */
  private def serialize(root: VectorSchemaRoot): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val channel = new WriteChannel(Channels.newChannel(bytes))
    val batch = new VectorUnloader(root).getRecordBatch
    try {
      MessageSerializer.serialize(channel, batch)
      ArrowStreamWriter.writeEndOfStream(channel, IpcOption.DEFAULT)
    } finally {
      batch.close()
    }
    bytes.toByteArray
  }

  private def micros(value: Long, unit: TimeUnit): Long = unit match {
    case TimeUnit.SECOND => Math.multiplyExact(value, 1000000L)
    case TimeUnit.MILLISECOND => Math.multiplyExact(value, 1000L)
    case TimeUnit.MICROSECOND => value
    case TimeUnit.NANOSECOND => Math.floorDiv(value, 1000L)
  }

  private def writer(column: AdbcColumn, source: ArrowType, timestampAsString: Boolean): Writer =
    column.tTypeId match {
      case TTypeId.BOOLEAN_TYPE =>
        (s, d, i, o) => d.asInstanceOf[BitVector].setSafe(o, s.asInstanceOf[BitVector].get(i))
      case TTypeId.SMALLINT_TYPE =>
        (s, d, i, o) => d.asInstanceOf[SmallIntVector]
          .setSafe(o, s.asInstanceOf[BaseIntVector].getValueAsLong(i).toShort)
      case TTypeId.INT_TYPE =>
        (s, d, i, o) => d.asInstanceOf[IntVector]
          .setSafe(o, s.asInstanceOf[BaseIntVector].getValueAsLong(i).toInt)
      case TTypeId.BIGINT_TYPE =>
        (s, d, i, o) => d.asInstanceOf[BigIntVector]
          .setSafe(o, s.asInstanceOf[BaseIntVector].getValueAsLong(i))
      case TTypeId.FLOAT_TYPE =>
        (s, d, i, o) => d.asInstanceOf[Float4Vector].setSafe(o, s.asInstanceOf[Float4Vector].get(i))
      case TTypeId.DOUBLE_TYPE =>
        (s, d, i, o) => d.asInstanceOf[Float8Vector].setSafe(o, s.asInstanceOf[Float8Vector].get(i))
      case TTypeId.DECIMAL_TYPE =>
        // AdbcSchema only advertises DECIMAL for a 128-bit source, so both sides are DecimalVector
        (s, d, i, o) => d.asInstanceOf[DecimalVector].copyFromSafe(i, o, s)
      case TTypeId.DATE_TYPE =>
        source match {
          case t: ArrowType.Date if t.getUnit == DateUnit.DAY =>
            (s, d, i, o) => d.asInstanceOf[DateDayVector].copyFromSafe(i, o, s)
          case _ =>
            (s, d, i, o) => d.asInstanceOf[DateDayVector].setSafe(
              o,
              Math.floorDiv(s.asInstanceOf[DateMilliVector].get(i), 86400000L).toInt)
        }
      case TTypeId.TIMESTAMP_TYPE if !timestampAsString =>
        val unit = source.asInstanceOf[ArrowType.Timestamp].getUnit
        (s, d, i, o) => d.asInstanceOf[TimeStampMicroTZVector]
          .setSafe(o, micros(s.asInstanceOf[TimeStampVector].get(i), unit))
      case _ =>
        // STRING, and TIMESTAMP as text: UTF-8 bytes of the value's text form (the row path's form)
        source match {
          case _: ArrowType.Utf8 =>
            (s, d, i, o) => d.asInstanceOf[VarCharVector].copyFromSafe(i, o, s)
          case _ =>
            (s, d, i, o) => d.asInstanceOf[VarCharVector].setSafe(
              o,
              column.read(s, i).toString.getBytes(StandardCharsets.UTF_8))
        }
    }
}

/** The Thrift side of an Arrow result: one serialized batch in one binary column. */
object AdbcArrowTRowSet {

  def apply(batch: Array[Byte]): TRowSet = {
    val values = new JArrayList[ByteBuffer](1)
    values.add(ByteBuffer.wrap(batch))
    val rowSet = new TRowSet(0, new JArrayList[TRow](1))
    rowSet.addToColumns(
      TColumn.binaryVal(new TBinaryColumn(values, ByteBuffer.wrap(Array.empty[Byte]))))
    rowSet
  }

  /** No more batches: the client treats a rowset without columns as the end of the result. */
  def empty: TRowSet = ThriftUtils.newEmptyRowSet
}
