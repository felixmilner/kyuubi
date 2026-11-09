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

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.util.{ArrayList => JArrayList, Collections}

import scala.collection.JavaConverters._

import org.apache.arrow.vector.{BaseIntVector, BitVector, FieldVector, Float4Vector, Float8Vector}
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.result.TRowSetGenerator
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.util.RowSetUtils.formatLocalDateTime

/**
 * One result column: how it is described to Thrift clients and how a value is read from the Arrow
 * vector (as the Java type the Thrift column generators expect, or null).
 */
case class AdbcColumn(
    name: String,
    tTypeId: TTypeId,
    precision: Int,
    scale: Int,
    read: (FieldVector, Int) => Any)

object AdbcSchema {

  /**
   * `dictionaries` is the reader the batches come from: a dictionary-encoded column (DuckDB sends
   * every ENUM that way) holds indices, and the values live in the reader's dictionary, which is
   * looked up on each read because a later batch can carry a new one.
   */
  def columns(schema: Schema, dictionaries: DictionaryProvider): List[AdbcColumn] =
    schema.getFields.asScala.map(f => column(f, dictionaries)).toList

  def columns(schema: Schema): List[AdbcColumn] =
    columns(schema, new DictionaryProvider.MapDictionaryProvider())

  private def nullSafe(f: (FieldVector, Int) => Any): (FieldVector, Int) => Any =
    (v, i) => if (v.isNull(i)) null else f(v, i)

  private def string(f: (FieldVector, Int) => String): (FieldVector, Int) => Any = nullSafe(f)

  /** Strings render the object's own text form; used for types without a Thrift column of their own. */
  private val asText: (FieldVector, Int) => Any = nullSafe((v, i) => String.valueOf(v.getObject(i)))

  def column(field: Field): AdbcColumn =
    column(field, new DictionaryProvider.MapDictionaryProvider())

  def column(field: Field, dictionaries: DictionaryProvider): AdbcColumn = {
    val name = field.getName
    def col(id: TTypeId, read: (FieldVector, Int) => Any, p: Int = 0, s: Int = 0): AdbcColumn =
      AdbcColumn(name, id, p, s, read)
    if (field.getDictionary != null) {
      // the field's own type is the index type; the client gets the dictionary's values as text
      val dictionaryId = field.getDictionary.getId
      col(TTypeId.STRING_TYPE, nullSafe { (v, i) =>
        val dictionary = dictionaries.lookup(dictionaryId)
        if (dictionary == null) {
          throw KyuubiSQLException(
            s"Column $name is dictionary-encoded but the reader has no dictionary $dictionaryId")
        }
        String.valueOf(dictionary.getVector.getObject(
          v.asInstanceOf[BaseIntVector].getValueAsLong(i).toInt))
      })
    } else field.getType match {
      case _: ArrowType.Bool =>
        // read the vector directly: this runs once per cell, and the text round trip it replaces
        // (found by the M2(d) benchmark) boxed, formatted and parsed every value
        col(
          TTypeId.BOOLEAN_TYPE,
          nullSafe((v, i) => java.lang.Boolean.valueOf(v.asInstanceOf[BitVector].get(i) == 1)))
      case t: ArrowType.Int =>
        def long(v: FieldVector, i: Int): Long = v.asInstanceOf[BaseIntVector].getValueAsLong(i)
        (t.getBitWidth, t.getIsSigned) match {
          case (8, _) | (16, true) =>
            col(TTypeId.SMALLINT_TYPE, nullSafe((v, i) => java.lang.Short.valueOf(long(v, i).toShort)))
          case (16, false) | (32, true) =>
            col(TTypeId.INT_TYPE, nullSafe((v, i) => java.lang.Integer.valueOf(long(v, i).toInt)))
          case (32, false) | (64, true) =>
            col(TTypeId.BIGINT_TYPE, nullSafe((v, i) => java.lang.Long.valueOf(long(v, i))))
          case _ =>
            col(TTypeId.STRING_TYPE, string((v, i) => java.lang.Long.toUnsignedString(long(v, i))))
        }
      case t: ArrowType.FloatingPoint =>
        t.getPrecision match {
          case FloatingPointPrecision.SINGLE =>
            col(
              TTypeId.FLOAT_TYPE,
              nullSafe((v, i) => java.lang.Float.valueOf(v.asInstanceOf[Float4Vector].get(i))))
          case FloatingPointPrecision.DOUBLE =>
            col(
              TTypeId.DOUBLE_TYPE,
              nullSafe((v, i) => java.lang.Double.valueOf(v.asInstanceOf[Float8Vector].get(i))))
          case _ => col(TTypeId.STRING_TYPE, asText)
        }
      case _: ArrowType.Utf8 | _: ArrowType.LargeUtf8 | _: ArrowType.Utf8View =>
        col(TTypeId.STRING_TYPE, string((v, i) => v.getObject(i).toString))
      case _: ArrowType.Binary | _: ArrowType.LargeBinary | _: ArrowType.FixedSizeBinary |
          _: ArrowType.BinaryView =>
        // Rendered as DuckDB renders a BLOB as text (non-printable bytes as \xHH), which is what
        // the existing JDBC-engine path returned to clients.
        col(TTypeId.STRING_TYPE, string((v, i) => blobToString(v.getObject(i).asInstanceOf[Array[Byte]])))
      case d: ArrowType.Decimal if d.getBitWidth == 128 && d.getPrecision <= 38 =>
        // the Arrow client only decodes 128-bit decimals; wider ones go as text in both encodings
        col(
          TTypeId.DECIMAL_TYPE,
          string((v, i) => v.getObject(i).asInstanceOf[java.math.BigDecimal].toPlainString),
          d.getPrecision,
          d.getScale)
      case d: ArrowType.Date if d.getUnit == DateUnit.DAY =>
        col(TTypeId.DATE_TYPE, string((v, i) => LocalDate.ofEpochDay(v.getObject(i).asInstanceOf[Integer].toLong).toString))
      case _: ArrowType.Date =>
        col(TTypeId.DATE_TYPE, string((v, i) => v.getObject(i).asInstanceOf[LocalDateTime].toLocalDate.toString))
      case t: ArrowType.Timestamp =>
        val text = string((v, i) => v.getObject(i) match {
          case ldt: LocalDateTime => formatLocalDateTime(ldt)
          case epoch: java.lang.Long => formatLocalDateTime(epochToLocalDateTime(epoch, t.getUnit))
          case other => String.valueOf(other)
        })
        // The Arrow client only decodes microsecond timestamps, so nanoseconds would lose digits
        // there but not in rows. Sent as text in both encodings they stay exact and identical.
        if (t.getUnit == TimeUnit.NANOSECOND) col(TTypeId.STRING_TYPE, text)
        else col(TTypeId.TIMESTAMP_TYPE, text)
      case t: ArrowType.Time =>
        col(TTypeId.STRING_TYPE, string((v, i) => v.getObject(i) match {
          case n: java.lang.Number => timeToString(n.longValue, t.getUnit)
          case other => String.valueOf(other)
        }))
      case _: ArrowType.Interval | _: ArrowType.Duration =>
        col(TTypeId.STRING_TYPE, nullSafe((v, i) => AdbcValueText.intervalText(v.getObject(i))))
      case t if AdbcValueText.isNested(t) =>
        // list, struct, map (and a fixed-size list): JSON text, ENUMs inside it decoded
        col(TTypeId.STRING_TYPE, nullSafe(AdbcValueText.nested(field, dictionaries)))
      case _ =>
        // union, null and anything else without a Thrift type: the object's text form
        col(TTypeId.STRING_TYPE, asText)
    }
  }

  private def epochToLocalDateTime(epoch: Long, unit: TimeUnit): LocalDateTime = {
    val instant = unit match {
      case TimeUnit.SECOND => Instant.ofEpochSecond(epoch)
      case TimeUnit.MILLISECOND => Instant.ofEpochMilli(epoch)
      case TimeUnit.MICROSECOND =>
        Instant.ofEpochSecond(Math.floorDiv(epoch, 1000000L), Math.floorMod(epoch, 1000000L) * 1000L)
      case TimeUnit.NANOSECOND =>
        Instant.ofEpochSecond(Math.floorDiv(epoch, 1000000000L), Math.floorMod(epoch, 1000000000L))
    }
    LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
  }

  private def timeToString(value: Long, unit: TimeUnit): String = {
    val nanos = unit match {
      case TimeUnit.SECOND => value * 1000000000L
      case TimeUnit.MILLISECOND => value * 1000000L
      case TimeUnit.MICROSECOND => value * 1000L
      case TimeUnit.NANOSECOND => value
    }
    LocalTime.ofNanoOfDay(nanos).toString
  }

  private[schema] def blobToString(bytes: Array[Byte]): String = {
    val sb = new java.lang.StringBuilder(bytes.length)
    bytes.foreach { b =>
      val c = b & 0xff
      if (c >= 32 && c < 127 && c != '\\') sb.append(c.toChar)
      else sb.append("\\x").append(f"$c%02X")
    }
    sb.toString
  }

  def toTTableSchema(columns: Seq[AdbcColumn]): TTableSchema = {
    val tTableSchema = new TTableSchema()
    tTableSchema.setColumns(new JArrayList[TColumnDesc]())
    columns.zipWithIndex.foreach { case (c, pos) =>
      val typeEntry = new TPrimitiveTypeEntry(c.tTypeId)
      val qualifiers = new TTypeQualifiers()
      qualifiers.setQualifiers(c.tTypeId match {
        case TTypeId.DECIMAL_TYPE =>
          Map(
            TCLIServiceConstants.PRECISION -> TTypeQualifierValue.i32Value(c.precision),
            TCLIServiceConstants.SCALE -> TTypeQualifierValue.i32Value(c.scale)).asJava
        case TTypeId.TIMESTAMP_TYPE =>
          // the Arrow client needs the zone to rebuild a timestamp column; rows ignore it
          Map(AdbcArrowTypes.TimeZoneQualifier ->
            TTypeQualifierValue.stringValue(AdbcArrowTypes.TimestampZone)).asJava
        case _ => Collections.emptyMap[String, TTypeQualifierValue]()
      })
      typeEntry.setTypeQualifiers(qualifiers)
      val tTypeDesc = new TTypeDesc()
      tTypeDesc.addToTypes(TTypeEntry.primitiveEntry(typeEntry))
      val desc = new TColumnDesc()
      desc.setColumnName(c.name)
      desc.setTypeDesc(tTypeDesc)
      desc.setPosition(pos)
      tTableSchema.addToColumns(desc)
    }
    tTableSchema
  }
}

object AdbcTRowSetGenerator extends TRowSetGenerator[Seq[AdbcColumn], Seq[Any], TTypeId] {

  override def getColumnSizeFromSchemaType(schema: Seq[AdbcColumn]): Int = schema.length

  override def getColumnType(schema: Seq[AdbcColumn], ordinal: Int): TTypeId =
    schema(ordinal).tTypeId

  override protected def isColumnNullAt(row: Seq[Any], ordinal: Int): Boolean = row(ordinal) == null

  override protected def getColumnAs[T](row: Seq[Any], ordinal: Int): T =
    row(ordinal).asInstanceOf[T]

  override def toTColumn(rows: Seq[Seq[Any]], ordinal: Int, typ: TTypeId): TColumn = typ match {
    case TTypeId.BOOLEAN_TYPE => asBooleanTColumn(rows, ordinal)
    case TTypeId.TINYINT_TYPE => asByteTColumn(rows, ordinal)
    case TTypeId.SMALLINT_TYPE => asShortTColumn(rows, ordinal)
    case TTypeId.INT_TYPE => asIntegerTColumn(rows, ordinal)
    case TTypeId.BIGINT_TYPE => asLongTColumn(rows, ordinal)
    case TTypeId.FLOAT_TYPE => asFloatTColumn(rows, ordinal)
    case TTypeId.DOUBLE_TYPE => asDoubleTColumn(rows, ordinal)
    case _ => asStringTColumn(rows, ordinal)
  }

  override def toTColumnValue(row: Seq[Any], ordinal: Int, types: Seq[AdbcColumn]): TColumnValue =
    types(ordinal).tTypeId match {
      case TTypeId.BOOLEAN_TYPE => asBooleanTColumnValue(row, ordinal)
      case TTypeId.TINYINT_TYPE => asByteTColumnValue(row, ordinal)
      case TTypeId.SMALLINT_TYPE => asShortTColumnValue(row, ordinal)
      case TTypeId.INT_TYPE => asIntegerTColumnValue(row, ordinal)
      case TTypeId.BIGINT_TYPE => asLongTColumnValue(row, ordinal)
      case TTypeId.FLOAT_TYPE => asFloatTColumnValue(row, ordinal)
      case TTypeId.DOUBLE_TYPE => asDoubleTColumnValue(row, ordinal)
      case _ => asStringTColumnValue(row, ordinal)
    }
}
