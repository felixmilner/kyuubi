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

import java.time.Duration

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{FixedSizeListVector, ListVector, StructVector}
import org.apache.arrow.vector.dictionary.{Dictionary, DictionaryProvider}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, Field, FieldType}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId

/**
 * Text of intervals and nested values. The expected strings are what DuckDB itself gives
 * (`CAST(x AS VARCHAR)` for intervals, `to_json(x)` for nested values), taken from the probe
 * `m2/ValueOracle.java`; the end-to-end check against DuckDB over Thrift is
 * `m2/NestedTextTest.java`.
 */
class AdbcValueTextSuite extends KyuubiFunSuite {

  private def field(name: String, t: ArrowType, children: Field*): Field =
    new Field(name, new FieldType(true, t, null), java.util.Arrays.asList(children: _*))

  private def required(name: String, t: ArrowType, children: Field*): Field =
    new Field(name, new FieldType(false, t, null), java.util.Arrays.asList(children: _*))

  private val int32 = new ArrowType.Int(32, true)
  private val uint8 = new ArrowType.Int(8, false)

  private def withAllocator[T](body: BufferAllocator => T): T = {
    val allocator = new RootAllocator(Long.MaxValue)
    try body(allocator)
    finally allocator.close()
  }

  private def vector[T <: FieldVector](f: Field, allocator: BufferAllocator): T = {
    val v = f.createVector(allocator)
    v.allocateNew()
    v.asInstanceOf[T]
  }

  private def reads(f: Field, v: FieldVector, provider: DictionaryProvider, rows: Int): Seq[Any] = {
    val column = AdbcSchema.column(f, provider)
    assert(column.tTypeId === TTypeId.STRING_TYPE)
    (0 until rows).map(i => column.read(v, i))
  }

  private def reads(f: Field, v: FieldVector, rows: Int): Seq[Any] =
    reads(f, v, new DictionaryProvider.MapDictionaryProvider(), rows)

  test("interval text follows DuckDB's cast to VARCHAR") {
    def h(n: Long) = Duration.ofHours(n)
    def i(months: Long, days: Long, time: Duration) = AdbcValueText.interval(months, days, time)
    assert(i(0, 1, Duration.ZERO) === "1 day")
    assert(i(0, 2, Duration.ZERO) === "2 days")
    assert(i(14, 0, Duration.ZERO) === "1 year 2 months")
    assert(i(0, 0, h(100)) === "100:00:00")
    assert(i(0, -3, Duration.ZERO) === "-3 days")
    assert(i(0, 0, Duration.ofMinutes(-90)) === "-01:30:00")
    assert(i(14, 3, Duration.ofHours(4).plusMinutes(5).plusSeconds(6).plusMillis(789)) ===
      "1 year 2 months 3 days 04:05:06.789")
    assert(i(0, 0, Duration.ofMillis(1500)) === "00:00:01.5")
    assert(i(0, 0, Duration.ZERO) === "00:00:00")
    assert(i(-1, 5, Duration.ZERO) === "-1 month 5 days")
    assert(i(0, 0, Duration.ofNanos(1000)) === "00:00:00.000001")
    assert(i(0, 1, h(-36)) === "1 day -36:00:00")
    assert(i(-14, 0, Duration.ZERO) === "-1 year -2 months")
    assert(i(12, 0, Duration.ZERO) === "1 year")
  }

  test("an interval column reads through the Arrow vector") {
    withAllocator { allocator =>
      val f = field("iv", new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO))
      val v = vector[IntervalMonthDayNanoVector](f, allocator)
      try {
        v.setSafe(0, 14, 3, Duration.ofHours(4).plusMinutes(5).plusSeconds(6).toNanos)
        v.setNull(1)
        v.setSafe(2, 0, 0, 0L)
        v.setValueCount(3)
        assert(reads(f, v, 3) === Seq("1 year 2 months 3 days 04:05:06", null, "00:00:00"))
      } finally v.close()
    }
  }

  test("a list keeps nulls and empty lists, and a null list is null") {
    withAllocator { allocator =>
      val f = field("l", ArrowType.List.INSTANCE, field("item", int32))
      val l = vector[ListVector](f, allocator)
      try {
        val data = l.getDataVector.asInstanceOf[IntVector]
        l.startNewValue(0)
        data.setSafe(0, 1)
        data.setNull(1)
        data.setSafe(2, 3)
        l.endValue(0, 3)
        l.setNull(1)
        l.startNewValue(2)
        l.endValue(2, 0)
        data.setValueCount(3)
        l.setValueCount(3)
        assert(reads(f, l, 3) === Seq("[1,null,3]", null, "[]"))
      } finally l.close()
    }
  }

  test("a struct keeps its null members") {
    withAllocator { allocator =>
      val f = field(
        "s",
        ArrowType.Struct.INSTANCE,
        field("a", int32),
        field("b", ArrowType.Utf8.INSTANCE))
      val s = vector[StructVector](f, allocator)
      try {
        val a = s.getChildrenFromFields.get(0).asInstanceOf[IntVector]
        val b = s.getChildrenFromFields.get(1).asInstanceOf[VarCharVector]
        s.setIndexDefined(0)
        a.setSafe(0, 1) // b stays null
        s.setIndexDefined(1)
        a.setNull(1)
        b.setSafe(1, "x\"y".getBytes("UTF-8"))
        // row 2: the struct itself is null
        s.setValueCount(3)
        a.setValueCount(3)
        b.setValueCount(3)
        assert(reads(f, s, 3) === Seq("""{"a":1,"b":null}""", """{"a":null,"b":"x\"y"}""", null))
      } finally s.close()
    }
  }

  private def mapField(key: Field, value: Field): Field =
    field(
      "m",
      new ArrowType.Map(false),
      required("entries", ArrowType.Struct.INSTANCE, key, value))

  test("a map is a JSON object; a key that is not a string becomes one") {
    withAllocator { allocator =>
      val f = mapField(required("key", int32), field("value", int32))
      val m = vector[ListVector](f, allocator)
      try {
        val entries = m.getDataVector.asInstanceOf[StructVector]
        val keys = entries.getChildrenFromFields.get(0).asInstanceOf[IntVector]
        val values = entries.getChildrenFromFields.get(1).asInstanceOf[IntVector]
        m.startNewValue(0)
        entries.setIndexDefined(0)
        keys.setSafe(0, 1)
        values.setSafe(0, 2)
        entries.setIndexDefined(1)
        keys.setSafe(1, 3) // value stays null
        m.endValue(0, 2)
        m.startNewValue(1)
        m.endValue(1, 0)
        entries.setValueCount(2)
        keys.setValueCount(2)
        values.setValueCount(2)
        m.setValueCount(2)
        assert(reads(f, m, 2) === Seq("""{"1":2,"3":null}""", "{}"))
      } finally m.close()
    }
  }

  test("a map with list values and string keys") {
    withAllocator { allocator =>
      val f = mapField(
        required("key", ArrowType.Utf8.INSTANCE),
        field("value", ArrowType.List.INSTANCE, field("item", int32)))
      val m = vector[ListVector](f, allocator)
      try {
        val entries = m.getDataVector.asInstanceOf[StructVector]
        val keys = entries.getChildrenFromFields.get(0).asInstanceOf[VarCharVector]
        val values = entries.getChildrenFromFields.get(1).asInstanceOf[ListVector]
        val items = values.getDataVector.asInstanceOf[IntVector]
        m.startNewValue(0)
        entries.setIndexDefined(0)
        keys.setSafe(0, "a".getBytes("UTF-8"))
        values.startNewValue(0)
        items.setSafe(0, 1)
        items.setSafe(1, 2)
        values.endValue(0, 2)
        entries.setIndexDefined(1)
        keys.setSafe(1, "b".getBytes("UTF-8"))
        values.startNewValue(1)
        values.endValue(1, 0)
        m.endValue(0, 2)
        items.setValueCount(2)
        values.setValueCount(2)
        keys.setValueCount(2)
        entries.setValueCount(2)
        m.setValueCount(1)
        assert(reads(f, m, 1) === Seq("""{"a":[1,2],"b":[]}"""))
      } finally m.close()
    }
  }

  test("ENUMs nested in lists, structs and maps are decoded by their own dictionary") {
    withAllocator { allocator =>
      def labels(id: Long, values: String*): Dictionary = {
        val v = new VarCharVector(s"labels$id", allocator)
        v.allocateNew()
        values.zipWithIndex.foreach { case (l, i) => v.setSafe(i, l.getBytes("UTF-8")) }
        v.setValueCount(values.size)
        new Dictionary(v, new DictionaryEncoding(id, false, uint8))
      }
      def encoded(name: String, id: Long): Field =
        new Field(
          name,
          new FieldType(true, uint8, new DictionaryEncoding(id, false, uint8)),
          java.util.Collections.emptyList[Field]())
      val d0 = labels(0L, "a", "b", "c")
      val d1 = labels(1L, "x", "y")
      val provider = new DictionaryProvider.MapDictionaryProvider(d0, d1)
      val f = field("s", ArrowType.Struct.INSTANCE,
        field("l", ArrowType.List.INSTANCE, encoded("item", 0L)),
        encoded("e", 1L))
      val s = vector[StructVector](f, allocator)
      try {
        val l = s.getChildrenFromFields.get(0).asInstanceOf[ListVector]
        val items = l.getDataVector.asInstanceOf[UInt1Vector]
        val e = s.getChildrenFromFields.get(1).asInstanceOf[UInt1Vector]
        s.setIndexDefined(0)
        l.startNewValue(0)
        items.setSafe(0, 2)
        items.setSafe(1, 1)
        l.endValue(0, 2)
        e.setSafe(0, 1)
        items.setValueCount(2)
        l.setValueCount(1)
        e.setValueCount(1)
        s.setValueCount(1)
        assert(reads(f, s, provider, 1) === Seq("""{"l":["c","b"],"e":"y"}"""))

        // without the dictionary the read fails instead of showing indices
        val missing = AdbcSchema.column(f, new DictionaryProvider.MapDictionaryProvider())
        val error = intercept[org.apache.kyuubi.KyuubiSQLException](missing.read(s, 0))
        assert(error.getMessage.contains("dictionary"))
      } finally {
        s.close()
        d0.getVector.close()
        d1.getVector.close()
      }
    }
  }

  test("leaves inside a list read as they do in a column of their own") {
    withAllocator { allocator =>
      def one[T <: FieldVector](t: ArrowType)(fill: T => Unit): String = {
        val f = field("l", ArrowType.List.INSTANCE, field("item", t))
        val l = vector[ListVector](f, allocator)
        try {
          val data = l.getDataVector.asInstanceOf[T]
          l.startNewValue(0)
          fill(data)
          l.endValue(0, 1)
          data.setValueCount(1)
          l.setValueCount(1)
          reads(f, l, 1).head.asInstanceOf[String]
        } finally l.close()
      }
      assert(one[DateDayVector](new ArrowType.Date(DateUnit.DAY))(_.setSafe(0, 18263)) ===
        """["2020-01-02"]""")
      assert(one[TimeStampMicroVector](new ArrowType.Timestamp(TimeUnit.MICROSECOND, null))(
        _.setSafe(0, 1577934245123456L)) === """["2020-01-02 03:04:05.123456"]""")
      assert(one[TimeMicroVector](new ArrowType.Time(TimeUnit.MICROSECOND, 64))(
        _.setSafe(0, 3723000000L)) === """["01:02:03"]""")
      assert(one[VarBinaryVector](ArrowType.Binary.INSTANCE)(
        _.setSafe(0, Array[Byte](1, -1))) === """["\\x01\\xFF"]""")
      assert(one[DecimalVector](ArrowType.Decimal.createDecimal(5, 2, 128))(
        _.setSafe(0, new java.math.BigDecimal("1.50"))) === "[1.50]")
      assert(one[Float8Vector](new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))(
        _.setSafe(0, 2.0)) === "[2.0]")
      assert(one[Float8Vector](new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))(
        _.setSafe(0, Double.NaN)) === """["NaN"]""")
      assert(one[Float8Vector](new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))(
        _.setSafe(0, Double.NegativeInfinity)) === """["-Infinity"]""")
      assert(one[BitVector](ArrowType.Bool.INSTANCE)(_.setSafe(0, 1)) === "[true]")
      assert(one[VarCharVector](ArrowType.Utf8.INSTANCE)(
        _.setSafe(0, "a\"b\\c\nd\u0001".getBytes("UTF-8"))) ===
        "[\"a\\\"b\\\\c\\nd" + "\\" + "u0001\"]") // [<"a\"b\\c\nd\u0001">] as JSON text
      assert(one[IntervalMonthDayNanoVector](new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO))(
        _.setSafe(0, 0, 1, 0L)) === """["1 day"]""")
    }
  }

  test("a fixed-size list is a JSON array too") {
    withAllocator { allocator =>
      val f = field("a", new ArrowType.FixedSizeList(3), field("item", int32))
      val a = vector[FixedSizeListVector](f, allocator)
      try {
        val data = a.getDataVector.asInstanceOf[IntVector]
        a.setNotNull(0)
        (0 until 3).foreach(k => data.setSafe(k, k + 1))
        data.setValueCount(3)
        a.setValueCount(1)
        assert(reads(f, a, 1) === Seq("[1,2,3]"))
      } finally a.close()
    }
  }
}
