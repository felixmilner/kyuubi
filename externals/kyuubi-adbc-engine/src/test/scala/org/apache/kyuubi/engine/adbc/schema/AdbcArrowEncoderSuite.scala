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

import java.io.ByteArrayInputStream
import java.math.BigDecimal
import java.nio.channels.Channels
import java.sql.Timestamp

import scala.collection.JavaConverters._

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.jdbc.hive.{JdbcColumnAttributes, KyuubiArrowQueryResultSet}
import org.apache.kyuubi.jdbc.hive.arrow.{ArrowColumnVector, ArrowColumnarBatch, ArrowUtils}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TBinaryColumn, TTypeId}

/**
 * Encodes driver-shaped batches with [[AdbcArrowEncoder]] and decodes them with the real client
 * classes (kyuubi-hive-jdbc), the way `KyuubiArrowQueryResultSet.next()` does, then compares each
 * value with what the row path (`AdbcColumn.read`) gives for the same cell.
 *
 * Limit: the client's classes run here on the ADBC engine's Arrow version, not the client's own
 * (16.0.0). The IPC wire format is the same; the end-to-end check over Thrift is what covers a
 * real client.
 */
class AdbcArrowEncoderSuite extends KyuubiFunSuite {

  private val Rows = 5
  private val NullRow = 1

  private def field(name: String, t: ArrowType): Field =
    new Field(name, new FieldType(true, t, null), java.util.Collections.emptyList[Field]())

  private def unsigned(bits: Int) = new ArrowType.Int(bits, false)
  private def signed(bits: Int) = new ArrowType.Int(bits, true)

  private val sourceFields: Seq[Field] = Seq(
    field("bool", ArrowType.Bool.INSTANCE),
    field("i8", signed(8)),
    field("i16", signed(16)),
    field("i32", signed(32)),
    field("i64", signed(64)),
    field("u8", unsigned(8)),
    field("u16", unsigned(16)),
    field("u32", unsigned(32)),
    field("u64", unsigned(64)),
    field("f32", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
    field("f64", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
    field("utf8", ArrowType.Utf8.INSTANCE),
    field("large_utf8", ArrowType.LargeUtf8.INSTANCE),
    field("binary", ArrowType.Binary.INSTANCE),
    field("dec128", ArrowType.Decimal.createDecimal(10, 2, 128)),
    field("dec256", ArrowType.Decimal.createDecimal(40, 2, 256)),
    field("date32", new ArrowType.Date(DateUnit.DAY)),
    field("ts_us", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
    field("ts_us_utc", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
    field("ts_ms", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
    field("ts_ns", new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)),
    field("time_us", new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
    new Field(
      "list_i32",
      new FieldType(true, ArrowType.List.INSTANCE, null),
      java.util.Arrays.asList(field("item", signed(32)))),
    field("null_col", ArrowType.Null.INSTANCE))

  private def fillSource(root: VectorSchemaRoot): Unit = {
    root.allocateNew()
    def vec[T <: FieldVector](name: String): T = root.getVector(name).asInstanceOf[T]
    for (i <- 0 until Rows if i != NullRow) {
      vec[BitVector]("bool").setSafe(i, i % 2)
      vec[TinyIntVector]("i8").setSafe(i, (i - 3).toByte)
      vec[SmallIntVector]("i16").setSafe(i, (i * 1000 - 2000).toShort)
      vec[IntVector]("i32").setSafe(i, i * 100000 - 200000)
      vec[BigIntVector]("i64").setSafe(i, i * 10000000000L - 20000000000L)
      vec[UInt1Vector]("u8").setSafe(i, 250 + i)
      vec[UInt2Vector]("u16").setSafe(i, 65530 + i)
      vec[UInt4Vector]("u32").setSafe(i, (4294967290L + i).toInt) // unsigned: the Int carries the bit pattern
      vec[UInt8Vector]("u64").setSafe(i, -1L - i) // above Long.MaxValue
      vec[Float4Vector]("f32").setSafe(i, i + 0.5f)
      vec[Float8Vector]("f64").setSafe(i, i + 0.25)
      vec[VarCharVector]("utf8").setSafe(i, s"héllo $i".getBytes("UTF-8"))
      vec[LargeVarCharVector]("large_utf8").setSafe(i, s"large $i".getBytes("UTF-8"))
      vec[VarBinaryVector]("binary").setSafe(i, Array[Byte](0, 65, 92, 127, -1, i.toByte))
      vec[DecimalVector]("dec128").setSafe(i, new BigDecimal(s"${12345 + i}.67"))
      vec[Decimal256Vector]("dec256").setSafe(i, new BigDecimal(s"${12345 + i}.67"))
      vec[DateDayVector]("date32").setSafe(i, 19000 + i)
      vec[TimeStampMicroVector]("ts_us").setSafe(i, 1700000000123456L + i)
      vec[TimeStampMicroTZVector]("ts_us_utc").setSafe(i, 1700000000123456L + i)
      vec[TimeStampMilliVector]("ts_ms").setSafe(i, 1700000000123L + i)
      vec[TimeStampNanoVector]("ts_ns").setSafe(i, 1700000000123456789L + i)
      vec[TimeMicroVector]("time_us").setSafe(i, 3600000000L + i)
      val list = vec[ListVector]("list_i32")
      val writer = list.getWriter
      writer.setPosition(i)
      writer.startList()
      writer.integer().writeInt(i)
      writer.integer().writeInt(i + 1)
      writer.endList()
    }
    root.getFieldVectors.asScala.foreach(_.setValueCount(Rows))
    root.setRowCount(Rows)
  }

  /** What the client does with the Thrift schema: its Arrow schema and per-column attributes. */
  private def clientView(columns: List[AdbcColumn], timestampAsString: Boolean)
      : (Schema, List[TTypeId], List[JdbcColumnAttributes]) = {
    val tSchema = AdbcSchema.toTTableSchema(columns)
    val entries = tSchema.getColumns.asScala.map(_.getTypeDesc.getTypes.get(0).getPrimitiveEntry)
    val ids = entries.map(_.getType).toList
    val attrs = entries.map(KyuubiArrowQueryResultSet.getColumnAttributes).toList
    // the client's own convertToStringType
    val arrowIds = ids.map {
      case TTypeId.ARRAY_TYPE | TTypeId.MAP_TYPE | TTypeId.STRUCT_TYPE => TTypeId.STRING_TYPE
      case TTypeId.TIMESTAMP_TYPE if timestampAsString => TTypeId.STRING_TYPE
      case other => other
    }
    val schema = ArrowUtils.toArrowSchema(
      columns.map(_.name).asJava,
      arrowIds.asJava,
      attrs.asJava)
    (schema, ids, attrs)
  }

  private def checkRoundTrip(timestampAsString: Boolean, maxRows: Int): Unit = {
    val allocator: BufferAllocator = new RootAllocator(Long.MaxValue)
    var failed = true
    try {
      val source = VectorSchemaRoot.create(new Schema(sourceFields.asJava), allocator)
      try {
        fillSource(source)
        val columns = AdbcSchema.columns(source.getSchema)
        val encoder = new AdbcArrowEncoder(
          columns,
          source.getSchema,
          timestampAsString,
          allocator,
          maxRows)
        val batches = try encoder.encode(source)
        finally encoder.close()
        assert(batches.size === (Rows + maxRows - 1) / maxRows)

        val (clientSchema, ids, attrs) = clientView(columns, timestampAsString)
        // the engine's target schema is what the client rebuilds from the Thrift schema
        val probe = new AdbcArrowEncoder(columns, source.getSchema, timestampAsString, allocator, 1)
        try assert(probe.targetSchema === clientSchema)
        finally probe.close()

        var rowIndex = 0
        batches.foreach { bytes =>
          val root = VectorSchemaRoot.create(clientSchema, allocator)
          val batch = MessageSerializer.deserializeRecordBatch(
            new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes))),
            allocator)
          new VectorLoader(root).load(batch)
          batch.close()
          try {
          val vectors = root.getFieldVectors.asScala.map(new ArrowColumnVector(_)).toArray
          val rows = new ArrowColumnarBatch(vectors, root.getRowCount).rowIterator()
          while (rows.hasNext) {
            val row = rows.next()
            columns.zipWithIndex.foreach { case (c, j) =>
              val expected = if (source.getVector(j).isNull(rowIndex)) null
              else c.read(source.getVector(j), rowIndex)
              val isNull = row.isNullAt(j)
              assert(isNull === (expected == null), s"null mismatch in ${c.name} row $rowIndex")
              if (!isNull) {
                val attr = attrs(j)
                val actual = row.get(j, ids(j), if (attr == null) null else attr.timeZone,
                  timestampAsString)
                assertSame(c, actual, expected, s"${c.name} row $rowIndex")
              }
            }
            rowIndex += 1
          }
          } finally root.close()
        }
        assert(rowIndex === Rows)
        failed = false
      } finally source.close()
    } finally {
      // a leak report after a failed assertion would only hide that assertion
      try allocator.close()
      catch { case _: IllegalStateException if failed => }
    }
  }

  private def assertSame(c: AdbcColumn, actual: Any, expected: Any, where: String): Unit =
    c.tTypeId match {
      case TTypeId.DECIMAL_TYPE =>
        assert(actual.asInstanceOf[BigDecimal].toPlainString === expected, where)
      case TTypeId.DATE_TYPE =>
        assert(actual.toString === expected, where)
      case TTypeId.TIMESTAMP_TYPE =>
        assert(
          actual.asInstanceOf[Timestamp] === Timestamp.valueOf(expected.toString),
          where)
      case _ => assert(actual === expected, where)
    }

  test("Arrow rowset decodes with the client to the same values as the row path") {
    checkRoundTrip(timestampAsString = false, maxRows = 8192)
  }

  test("Arrow rowset with timestamps as strings") {
    checkRoundTrip(timestampAsString = true, maxRows = 8192)
  }

  test("a batch larger than the cap is sliced into several fetches") {
    checkRoundTrip(timestampAsString = false, maxRows = 2)
    checkRoundTrip(timestampAsString = true, maxRows = 1)
  }

  test("types the client cannot decode are advertised and sent as text") {
    val columns = AdbcSchema.columns(new Schema(sourceFields.asJava))
    val byName = columns.map(c => c.name -> c.tTypeId).toMap
    assert(byName("dec128") === TTypeId.DECIMAL_TYPE)
    assert(byName("dec256") === TTypeId.STRING_TYPE)
    assert(byName("u64") === TTypeId.STRING_TYPE)
    assert(byName("i8") === TTypeId.SMALLINT_TYPE)
    assert(byName("time_us") === TTypeId.STRING_TYPE)
    assert(byName("ts_ns") === TTypeId.STRING_TYPE) // nanoseconds would lose digits as micros
    assert(byName("ts_ms") === TTypeId.TIMESTAMP_TYPE)
    assert(byName("list_i32") === TTypeId.STRING_TYPE)
    assert(byName("binary") === TTypeId.STRING_TYPE)
    assert(byName("null_col") === TTypeId.STRING_TYPE)
  }

  test("timestamp columns carry the session time zone for the client") {
    val columns = AdbcSchema.columns(new Schema(sourceFields.asJava))
    val tSchema = AdbcSchema.toTTableSchema(columns)
    val ts = tSchema.getColumns.asScala.find(_.getColumnName == "ts_us").get
    val qualifiers = ts.getTypeDesc.getTypes.get(0).getPrimitiveEntry.getTypeQualifiers
    assert(qualifiers.getQualifiers.get("session.timeZone").getStringValue === "UTC")
  }

  test("the Thrift row set carries exactly one binary value") {
    val rowSet = AdbcArrowTRowSet(Array[Byte](1, 2, 3))
    assert(rowSet.getColumnsSize === 1)
    val values = rowSet.getColumns.get(0).getBinaryVal.asInstanceOf[TBinaryColumn].getValues
    assert(values.size === 1)
    assert(values.get(0).array().toSeq === Seq[Byte](1, 2, 3))
    assert(AdbcArrowTRowSet.empty.getColumnsSize === 0)
  }
}
