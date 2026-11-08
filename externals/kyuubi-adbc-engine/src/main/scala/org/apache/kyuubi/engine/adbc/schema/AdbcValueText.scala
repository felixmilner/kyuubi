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

import java.time.{Duration, Period}

import scala.collection.JavaConverters._

import org.apache.arrow.vector.{FieldVector, PeriodDuration, ValueVector}
import org.apache.arrow.vector.complex.{FixedSizeListVector, ListVector, StructVector}
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

/**
 * The text of the values Thrift has no column type for: intervals, and lists, structs and maps.
 *
 * Nested values are rendered as JSON, the form DuckDB's own `to_json` gives, built from the Arrow
 * vectors instead of the vectors' `getObject` text (which drops null struct fields and shows
 * dates, times, timestamps, blobs and intervals in their storage forms). A leaf inside a nested
 * value reads as it would in a column of its own, so `SELECT d` and `SELECT [d]` agree on a
 * date's text. Maps are JSON objects; a key that is not a string becomes the text of its JSON
 * form.
 *
 * Reads must be made inside the native section: the vectors belong to the driver's result.
 */
private[schema] object AdbcValueText {

  private type Appender = (ValueVector, Int, java.lang.StringBuilder) => Unit

  /** Reads of a nested column: the JSON text of a non-null value. */
  def nested(field: Field, dictionaries: DictionaryProvider): (FieldVector, Int) => String = {
    val node = json(field, dictionaries)
    (v, i) => {
      val sb = new java.lang.StringBuilder()
      node(v, i, sb)
      sb.toString
    }
  }

  def isNested(t: ArrowType): Boolean = t match {
    case _: ArrowType.List | _: ArrowType.FixedSizeList | _: ArrowType.Struct | _: ArrowType.Map =>
      true
    case _ => false
  }

  private def json(field: Field, d: DictionaryProvider): Appender = {
    val inner: Appender =
      if (field.getDictionary != null) leaf(field, d, quoted = true)
      else field.getType match {
        case _: ArrowType.Map => map(field, d)
        case _: ArrowType.List => list(field, d)
        case _: ArrowType.FixedSizeList => list(field, d)
        case _: ArrowType.Struct => struct(field, d)
        case _: ArrowType.Bool | _: ArrowType.Int | _: ArrowType.Decimal =>
          leaf(field, d, quoted = false)
        case _: ArrowType.FloatingPoint => number(field, d)
        case _ => leaf(field, d, quoted = true)
      }
    (v, i, sb) => if (v.isNull(i)) sb.append("null") else inner(v, i, sb)
  }

  private def leaf(field: Field, d: DictionaryProvider, quoted: Boolean): Appender = {
    val read = AdbcSchema.column(field, d).read
    (v, i, sb) => {
      val text = String.valueOf(read(v.asInstanceOf[FieldVector], i))
      if (quoted) quote(sb, text) else sb.append(text)
    }
  }

  /** A float or double; JSON has no NaN or infinity, so those go as strings. */
  private def number(field: Field, d: DictionaryProvider): Appender = {
    val read = AdbcSchema.column(field, d).read
    (v, i, sb) => {
      val text = String.valueOf(read(v.asInstanceOf[FieldVector], i))
      if (text == "NaN" || text.endsWith("Infinity")) quote(sb, text) else sb.append(text)
    }
  }

  private def list(field: Field, d: DictionaryProvider): Appender = {
    val child = json(field.getChildren.get(0), d)
    def elements(data: ValueVector, start: Int, end: Int, sb: java.lang.StringBuilder): Unit = {
      sb.append('[')
      var j = start
      while (j < end) {
        if (j > start) sb.append(',')
        child(data, j, sb)
        j += 1
      }
      sb.append(']')
    }
    (v, i, sb) => v match {
      case l: ListVector =>
        elements(l.getDataVector, l.getElementStartIndex(i), l.getElementEndIndex(i), sb)
      case f: FixedSizeListVector =>
        elements(f.getDataVector, i * f.getListSize, (i + 1) * f.getListSize, sb)
    }
  }

  private def struct(field: Field, d: DictionaryProvider): Appender = {
    val members = field.getChildren.asScala.map(f => (f.getName, json(f, d))).toArray
    (v, i, sb) => {
      val children = v.asInstanceOf[StructVector].getChildrenFromFields
      sb.append('{')
      var k = 0
      while (k < members.length) {
        if (k > 0) sb.append(',')
        quote(sb, members(k)._1)
        sb.append(':')
        members(k)._2(children.get(k), i, sb)
        k += 1
      }
      sb.append('}')
    }
  }

  private def map(field: Field, d: DictionaryProvider): Appender = {
    val entry = field.getChildren.get(0)
    val key = json(entry.getChildren.get(0), d)
    val value = json(entry.getChildren.get(1), d)
    (v, i, sb) => {
      val list = v.asInstanceOf[ListVector]
      val children = list.getDataVector.asInstanceOf[StructVector].getChildrenFromFields
      val start = list.getElementStartIndex(i)
      val end = list.getElementEndIndex(i)
      sb.append('{')
      var j = start
      while (j < end) {
        if (j > start) sb.append(',')
        val k = new java.lang.StringBuilder()
        key(children.get(0), j, k)
        // a JSON object has string keys: a key that is not one becomes the text of its JSON form
        if (k.length > 0 && k.charAt(0) == '"') sb.append(k) else quote(sb, k.toString)
        sb.append(':')
        value(children.get(1), j, sb)
        j += 1
      }
      sb.append('}')
    }
  }

  private def quote(sb: java.lang.StringBuilder, s: String): java.lang.StringBuilder = {
    sb.append('"')
    var i = 0
    while (i < s.length) {
      s.charAt(i) match {
        case '"' => sb.append("\\\"")
        case '\\' => sb.append("\\\\")
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case '\b' => sb.append("\\b")
        case '\f' => sb.append("\\f")
        case c if c < 0x20 => sb.append(f"\\u${c.toInt}%04x")
        case c => sb.append(c)
      }
      i += 1
    }
    sb.append('"')
  }

  /** The text of an Arrow interval or duration value (a `getObject` result), in DuckDB's form. */
  def intervalText(value: Any): String = try {
    value match {
      case pd: PeriodDuration =>
        interval(pd.getPeriod.toTotalMonths, pd.getPeriod.getDays, pd.getDuration)
      case p: Period => interval(p.toTotalMonths, p.getDays, Duration.ZERO)
      case d: Duration => interval(0L, 0L, d)
      case other => String.valueOf(other)
    }
  } catch {
    case _: ArithmeticException => String.valueOf(value)
  }

  /**
   * `1 year 2 months 3 days 04:05:06.789`: years and months from the month count, days, then the
   * time of day with its own sign (hours can pass 24). Plural unless the count is 1 or -1; all
   * zero is `00:00:00`. This is how DuckDB casts an interval to text.
   */
  private[schema] def interval(months: Long, days: Long, time: Duration): String = {
    val parts = new java.util.ArrayList[String](4)
    def unit(n: Long, name: String): Unit =
      if (n != 0) parts.add(s"$n $name${if (math.abs(n) == 1) "" else "s"}")
    unit(months / 12, "year")
    unit(months % 12, "month")
    unit(days, "day")
    if (!time.isZero) parts.add(timeOfDay(time))
    if (parts.isEmpty) "00:00:00" else String.join(" ", parts)
  }

  private def timeOfDay(time: Duration): String = {
    val abs = time.abs()
    val seconds = abs.getSeconds
    val fraction =
      if (abs.getNano == 0) "" else "." + f"${abs.getNano}%09d".replaceAll("0+$", "")
    val sign = if (time.isNegative) "-" else ""
    sign + f"${seconds / 3600}%02d:${seconds % 3600 / 60}%02d:${seconds % 60}%02d" + fraction
  }
}
