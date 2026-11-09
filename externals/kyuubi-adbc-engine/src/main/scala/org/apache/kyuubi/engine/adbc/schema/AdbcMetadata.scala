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

import java.sql.Types
import java.util.regex.Pattern

import scala.collection.JavaConverters._

import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId

/** The result schemas of the metadata operations: the Hive/JDBC column lists the clients expect. */
object AdbcMetadataSchemas {

  private def column(name: String, id: TTypeId): AdbcColumn =
    AdbcColumn(name, id, 0, 0, (_, _) => null)

  private def string(name: String) = column(name, TTypeId.STRING_TYPE)
  private def int(name: String) = column(name, TTypeId.INT_TYPE)

  val Catalogs: List[AdbcColumn] = List(string(TABLE_CAT))

  val CurrentCatalog: List[AdbcColumn] = List(string(TABLE_CAT))

  val CurrentDatabase: List[AdbcColumn] = List(string(TABLE_SCHEM))

  val Schemas: List[AdbcColumn] = List(string(TABLE_SCHEM), string(TABLE_CATALOG))

  val TableTypes: List[AdbcColumn] = List(string(TABLE_TYPE))

  val Tables: List[AdbcColumn] = List(
    TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS, TYPE_CAT, TYPE_SCHEM, TYPE_NAME,
    SELF_REFERENCING_COL_NAME, REF_GENERATION).map(string)

  /** The 23 columns of the Spark engine's GetColumns, in its order and with its types. */
  val Columns: List[AdbcColumn] = List(
    string(TABLE_CAT),
    string(TABLE_SCHEM),
    string(TABLE_NAME),
    string(COLUMN_NAME),
    int(DATA_TYPE),
    string(TYPE_NAME),
    int(COLUMN_SIZE),
    column(BUFFER_LENGTH, TTypeId.TINYINT_TYPE),
    int(DECIMAL_DIGITS),
    int(NUM_PREC_RADIX),
    int(NULLABLE),
    string(REMARKS),
    string(COLUMN_DEF),
    int(SQL_DATA_TYPE),
    int(SQL_DATETIME_SUB),
    int(CHAR_OCTET_LENGTH),
    int(ORDINAL_POSITION),
    string(IS_NULLABLE),
    string(SCOPE_CATALOG),
    string(SCOPE_SCHEMA),
    string(SCOPE_TABLE),
    column(SOURCE_DATA_TYPE, TTypeId.SMALLINT_TYPE),
    string(IS_AUTO_INCREMENT))
}

/**
 * JDBC metadata patterns (`%` any run, `_` any one character, `\` escapes the next character).
 * Null, empty and `*` match everything, as in the other Kyuubi engines.
 */
object LikePattern {

  def isAll(pattern: String): Boolean =
    pattern == null || pattern.isEmpty || pattern == "*" || pattern == "%"

  def matcher(pattern: String): String => Boolean = {
    if (isAll(pattern)) {
      _ => true
    } else {
      val regex = toRegex(pattern)
      name => name != null && regex.matcher(name).matches()
    }
  }

  /** An exact name, not a pattern (JDBC catalogs): null and empty match everything. */
  def exact(name: String): String => Boolean =
    if (name == null || name.isEmpty) _ => true else _ == name

  /**
   * The pattern to hand to the driver, or null. A driver may not implement the escape, so a
   * pattern with a backslash is not passed on (the caller filters the result itself); `%` and `_`
   * mean the same everywhere. The caller always filters, so this only saves work.
   */
  def forDriver(pattern: String): String =
    if (isAll(pattern) || pattern.indexOf('\\') >= 0) null else pattern

  /** A catalog is an exact name here, so only a name without wildcards can be handed over. */
  def catalogForDriver(name: String): String =
    if (name == null || name.isEmpty || name.exists(c => c == '%' || c == '_' || c == '\\')) null
    else name

  private def toRegex(pattern: String): Pattern = {
    val sb = new java.lang.StringBuilder()
    var i = 0
    while (i < pattern.length) {
      pattern.charAt(i) match {
        case '\\' if i + 1 < pattern.length =>
          i += 1
          sb.append(Pattern.quote(pattern.charAt(i).toString))
        case '%' => sb.append(".*")
        case '_' => sb.append('.')
        case c => sb.append(Pattern.quote(c.toString))
      }
      i += 1
    }
    Pattern.compile(sb.toString, Pattern.DOTALL)
  }
}

/** One column of a table as `getObjects` reports it; everything but the name is optional. */
case class ObjectColumn(
    name: String,
    ordinal: Option[Int],
    remarks: Option[String],
    typeName: Option[String],
    dataType: Option[Int],
    columnSize: Option[Int],
    decimalDigits: Option[Int],
    radix: Option[Int],
    nullable: Option[Int],
    columnDef: Option[String],
    isAutoIncrement: Option[Boolean])

case class ObjectTable(name: String, tableType: Option[String], columns: Seq[ObjectColumn])

case class ObjectSchema(name: Option[String], tables: Seq[ObjectTable])

case class ObjectCatalog(name: Option[String], schemas: Seq[ObjectSchema])

/** Reads the result of `AdbcConnection.getObjects` into plain objects; no vector is kept. */
object AdbcObjects {

  /** Every batch of `reader`; the caller closes it. */
  def read(reader: ArrowReader): Seq[ObjectCatalog] = {
    val out = Seq.newBuilder[ObjectCatalog]
    val root = reader.getVectorSchemaRoot
    while (reader.loadNextBatch()) {
      val names = root.getVector("catalog_name")
      val schemas = root.getVector("catalog_db_schemas")
      var i = 0
      while (i < root.getRowCount) {
        out += ObjectCatalog(
          if (names == null) None else text(names.getObject(i)),
          if (schemas == null) Nil else schemasOf(schemas.getObject(i)))
        i += 1
      }
    }
    out.result()
  }

  // The vectors' getObject gives java lists for list vectors and maps for structs, with Text for
  // strings; a field a driver does not fill is a missing key or null.
  private def list(o: Any): Seq[java.util.Map[String, Any]] = o match {
    case l: java.util.List[_] =>
      l.asScala.collect {
        case m: java.util.Map[_, _] => m.asInstanceOf[java.util.Map[String, Any]]
      }
    case _ => Nil
  }

  private def text(o: Any): Option[String] = o match {
    case null => None
    case other => Some(other.toString)
  }

  private def int(o: Any): Option[Int] = o match {
    case n: java.lang.Number => Some(n.intValue)
    case _ => None
  }

  private def schemasOf(o: Any): Seq[ObjectSchema] = list(o).map { s =>
    ObjectSchema(text(s.get("db_schema_name")), tablesOf(s.get("db_schema_tables")))
  }

  private def tablesOf(o: Any): Seq[ObjectTable] = list(o).map { t =>
    ObjectTable(
      text(t.get("table_name")).orNull,
      text(t.get("table_type")),
      columnsOf(t.get("table_columns")))
  }

  private def columnsOf(o: Any): Seq[ObjectColumn] = list(o).map { c =>
    ObjectColumn(
      text(c.get("column_name")).orNull,
      int(c.get("ordinal_position")),
      text(c.get("remarks")),
      text(c.get("xdbc_type_name")),
      // DuckDB fills xdbc_sql_data_type (the JDBC type code) and leaves xdbc_data_type empty
      int(c.get("xdbc_data_type")).orElse(int(c.get("xdbc_sql_data_type"))),
      int(c.get("xdbc_column_size")),
      int(c.get("xdbc_decimal_digits")),
      int(c.get("xdbc_num_prec_radix")),
      int(c.get("xdbc_nullable")),
      text(c.get("xdbc_column_def")),
      c.get("xdbc_is_autoincrement") match {
        case b: java.lang.Boolean => Some(b.booleanValue)
        case _ => None
      })
  }
}

/** The driver calls the metadata operations need; the real one wraps an `AdbcConnection`. */
trait AdbcMetadataSource {

  /** `getObjects` at the given depth; null arguments mean no filter. */
  def objects(
      depth: AdbcMetadataSource.Depth,
      catalog: String,
      schema: String,
      table: String,
      tableTypes: Array[String],
      column: String): Seq[ObjectCatalog]

  def tableTypes(): Seq[String]

  /** The Arrow schema of one table, or None when the driver cannot say. */
  def tableSchema(catalog: String, schema: String, table: String): Option[Schema]
}

object AdbcMetadataSource {
  sealed trait Depth
  case object Catalogs extends Depth
  case object Schemas extends Depth
  case object Tables extends Depth
  case object All extends Depth
}

/** JDBC column attributes of a column, derived from the type the engine advertises for it. */
case class JdbcColumnType(
    dataType: Int,
    typeName: String,
    columnSize: Option[Int],
    decimalDigits: Option[Int],
    radix: Option[Int])

object JdbcTypes {

  /**
   * From the column's advertised Thrift type, so that metadata and result sets agree: a column a
   * query result calls BIGINT is BIGINT here too. Sizes follow JDBC (digits, characters).
   */
  def of(column: AdbcColumn): JdbcColumnType = column.tTypeId match {
    case TTypeId.BOOLEAN_TYPE => JdbcColumnType(Types.BOOLEAN, "BOOLEAN", Some(1), Some(0), None)
    case TTypeId.SMALLINT_TYPE =>
      JdbcColumnType(Types.SMALLINT, "SMALLINT", Some(5), Some(0), Some(10))
    case TTypeId.INT_TYPE => JdbcColumnType(Types.INTEGER, "INT", Some(10), Some(0), Some(10))
    case TTypeId.BIGINT_TYPE => JdbcColumnType(Types.BIGINT, "BIGINT", Some(19), Some(0), Some(10))
    case TTypeId.FLOAT_TYPE => JdbcColumnType(Types.FLOAT, "FLOAT", Some(7), Some(7), Some(2))
    case TTypeId.DOUBLE_TYPE => JdbcColumnType(Types.DOUBLE, "DOUBLE", Some(15), Some(15), Some(2))
    case TTypeId.DECIMAL_TYPE =>
      JdbcColumnType(Types.DECIMAL, "DECIMAL", Some(column.precision), Some(column.scale), Some(10))
    case TTypeId.DATE_TYPE => JdbcColumnType(Types.DATE, "DATE", Some(10), None, None)
    case TTypeId.TIMESTAMP_TYPE =>
      JdbcColumnType(Types.TIMESTAMP, "TIMESTAMP", Some(26), Some(6), None)
    case _ => JdbcColumnType(Types.VARCHAR, "STRING", Some(Int.MaxValue), None, None)
  }
}

/** The table types clients ask for are `TABLE` and `VIEW`; drivers name theirs differently. */
object TableTypes {

  /** `BASE TABLE` (DuckDB's information_schema) is a plain `TABLE`; other names are kept. */
  def normalize(driverType: String): String = {
    if (driverType == null) {
      null
    } else {
      driverType.trim.toUpperCase match {
        case "BASE TABLE" | "LOCAL TABLE" | "TABLE" => "TABLE"
        case other => other
      }
    }
  }

  /** Whether a table of `driverType` is wanted by a request for `requested` (null/empty: all). */
  def wanted(driverType: String, requested: Seq[String]): Boolean = {
    if (requested == null || requested.isEmpty) {
      true
    } else {
      val have = normalize(driverType)
      requested.exists(r => normalize(r) == have)
    }
  }
}

/** The rows of the metadata operations, from a [[AdbcMetadataSource]]. */
object AdbcMetadataQueries {

  import AdbcMetadataSource._

  def catalogs(source: AdbcMetadataSource): Seq[Seq[Any]] =
    source.objects(Catalogs, null, null, null, null, null)
      .flatMap(_.name).distinct.map(n => Seq(n))

  def tableTypes(source: AdbcMetadataSource): Seq[Seq[Any]] =
    source.tableTypes().map(TableTypes.normalize).distinct.sorted.map(t => Seq(t))

  def schemas(
      source: AdbcMetadataSource,
      catalog: String,
      schemaPattern: String): Seq[Seq[Any]] = {
    val inCatalog = LikePattern.exact(catalog)
    val matches = LikePattern.matcher(schemaPattern)
    for {
      c <- source.objects(
        Schemas, LikePattern.catalogForDriver(catalog), LikePattern.forDriver(schemaPattern),
        null, null, null)
      if inCatalog(c.name.orNull)
      s <- c.schemas
      if matches(s.name.orNull)
    } yield Seq(s.name.orNull, c.name.orNull)
  }

  def tables(
      source: AdbcMetadataSource,
      catalog: String,
      schemaPattern: String,
      tablePattern: String,
      tableTypes: Seq[String]): Seq[Seq[Any]] = {
    for {
      (c, s, t) <- tablesOf(source, Tables, catalog, schemaPattern, tablePattern, null)
      if TableTypes.wanted(t.tableType.orNull, tableTypes)
    } yield Seq(
      c.name.orNull, s.name.orNull, t.name, TableTypes.normalize(t.tableType.orNull),
      null, null, null, null, null, null)
  }

  private def tablesOf(
      source: AdbcMetadataSource,
      depth: Depth,
      catalog: String,
      schemaPattern: String,
      tablePattern: String,
      columnPattern: String): Seq[(ObjectCatalog, ObjectSchema, ObjectTable)] = {
    val inCatalog = LikePattern.exact(catalog)
    val schemaMatches = LikePattern.matcher(schemaPattern)
    val tableMatches = LikePattern.matcher(tablePattern)
    for {
      c <- source.objects(
        depth,
        LikePattern.catalogForDriver(catalog),
        LikePattern.forDriver(schemaPattern),
        LikePattern.forDriver(tablePattern),
        null,
        LikePattern.forDriver(columnPattern))
      if inCatalog(c.name.orNull)
      s <- c.schemas
      if schemaMatches(s.name.orNull)
      t <- s.tables
      if tableMatches(t.name)
    } yield (c, s, t)
  }

  /**
   * One row per column. Types come from the table's Arrow schema when the driver gives one (so they
   * match what `SELECT *` advertises), else from the `xdbc_*` fields of `getObjects`, else the
   * column is text. `onSchemaError` hears about a table whose schema could not be read.
   */
  def columns(
      source: AdbcMetadataSource,
      catalog: String,
      schemaPattern: String,
      tablePattern: String,
      columnPattern: String,
      onSchemaError: (ObjectTable, Throwable) => Unit = (_, _) => ()): Seq[Seq[Any]] = {
    val columnMatches = LikePattern.matcher(columnPattern)
    tablesOf(source, All, catalog, schemaPattern, tablePattern, columnPattern).flatMap {
      case (c, s, t) =>
        val wanted = t.columns.filter(col => columnMatches(col.name))
        val fields: Map[String, (org.apache.arrow.vector.types.pojo.Field, Int)] =
          if (wanted.isEmpty) {
            Map.empty
          } else {
            val schema =
              try source.tableSchema(c.name.orNull, s.name.orNull, t.name)
              catch {
                case e: Exception =>
                  onSchemaError(t, e)
                  None
              }
            schema.map(_.getFields.asScala.zipWithIndex.map { case (f, i) => f.getName -> (f, i) }
              .toMap).getOrElse(Map.empty)
          }
        wanted.zipWithIndex.map { case (col, index) =>
          columnRow(c.name.orNull, s.name.orNull, t.name, col, fields.get(col.name), index)
        }
    }
  }

  private def columnRow(
      catalog: String,
      schema: String,
      table: String,
      col: ObjectColumn,
      field: Option[(org.apache.arrow.vector.types.pojo.Field, Int)],
      index: Int): Seq[Any] = {
    val advertised = field.map { case (f, _) => AdbcSchema.column(f) }
    val jdbc = advertised.map(JdbcTypes.of)
    // The driver's own nullability first: DuckDB's getTableSchema (a `SELECT * ... LIMIT 0` schema)
    // marks every field nullable, but getObjects knows about NOT NULL.
    val nullable: Option[Boolean] =
      col.nullable.collect { case 0 => false; case 1 => true }.orElse(field.map(_._1.isNullable))
    val ordinal = col.ordinal.orElse(field.map(_._2 + 1)).getOrElse(index + 1)
    def boxed(v: Option[Int]): Any = v.map(Int.box).orNull
    Seq(
      catalog, // TABLE_CAT
      schema, // TABLE_SCHEM
      table, // TABLE_NAME
      col.name, // COLUMN_NAME
      Int.box(jdbc.map(_.dataType).orElse(col.dataType).getOrElse(Types.VARCHAR)), // DATA_TYPE
      col.typeName.orElse(jdbc.map(_.typeName)).getOrElse("STRING"), // TYPE_NAME
      boxed(jdbc.flatMap(_.columnSize).orElse(col.columnSize)), // COLUMN_SIZE
      null, // BUFFER_LENGTH
      boxed(jdbc.flatMap(_.decimalDigits).orElse(col.decimalDigits)), // DECIMAL_DIGITS
      boxed(jdbc.flatMap(_.radix).orElse(col.radix)), // NUM_PREC_RADIX
      Int.box(nullable.map(n => if (n) 1 else 0).getOrElse(2)), // NULLABLE (2: unknown)
      col.remarks.orNull, // REMARKS
      col.columnDef.orNull, // COLUMN_DEF
      null, // SQL_DATA_TYPE
      null, // SQL_DATETIME_SUB
      null, // CHAR_OCTET_LENGTH
      Int.box(ordinal), // ORDINAL_POSITION
      nullable.map(n => if (n) "YES" else "NO").getOrElse(""), // IS_NULLABLE
      null, // SCOPE_CATALOG
      null, // SCOPE_SCHEMA
      null, // SCOPE_TABLE
      null, // SOURCE_DATA_TYPE
      col.isAutoIncrement.map(a => if (a) "YES" else "NO").getOrElse("")) // IS_AUTO_INCREMENT
  }
}
