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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.sql.Types

import scala.collection.JavaConverters._

import org.apache.arrow.adbc.core.StandardSchemas
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.util.Text

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId

class AdbcMetadataSuite extends KyuubiFunSuite {

  private def field(name: String, t: ArrowType, nullable: Boolean = true): Field =
    new Field(name, new FieldType(nullable, t, null), java.util.Collections.emptyList[Field]())

  private val int32 = new ArrowType.Int(32, true)

  private def column(name: String, ordinal: Int, typeName: String = null): ObjectColumn =
    ObjectColumn(
      name, Some(ordinal), None, Option(typeName), None, None, None, None, None, None, None)

  /** A driver stand-in that ignores every filter, and remembers what it was asked. */
  private class FakeSource(
      catalogs: Seq[ObjectCatalog],
      schemas: Map[String, Schema] = Map.empty,
      types: Seq[String] = Nil,
      failSchemaFor: Set[String] = Set.empty) extends AdbcMetadataSource {
    var asked: Seq[(String, String, String, String)] = Nil
    var schemaCalls: Seq[String] = Nil

    override def objects(
        depth: AdbcMetadataSource.Depth,
        catalog: String,
        schema: String,
        table: String,
        tableTypes: Array[String],
        column: String): Seq[ObjectCatalog] = {
      asked = asked :+ ((catalog, schema, table, column))
      catalogs
    }

    override def tableTypes(): Seq[String] = types

    override def tableSchema(catalog: String, schema: String, table: String): Option[Schema] = {
      schemaCalls = schemaCalls :+ table
      if (failSchemaFor.contains(table)) throw new IllegalStateException(s"no schema for $table")
      schemas.get(table)
    }
  }

  private val warehouse = Seq(
    ObjectCatalog(
      Some("my_db"),
      Seq(
        ObjectSchema(
          Some("main"),
          Seq(
            ObjectTable("orders", Some("BASE TABLE"), Seq(column("id", 1), column("note", 2))),
            ObjectTable("orders_v", Some("VIEW"), Seq(column("id", 1))))),
        ObjectSchema(Some("raw_data"), Seq(ObjectTable("events", Some("BASE TABLE"), Nil))))),
    ObjectCatalog(
      Some("myXdb"),
      Seq(ObjectSchema(Some("main"), Seq(ObjectTable("other", Some("BASE TABLE"), Nil))))),
    ObjectCatalog(Some("system"), Nil))

  test("result schemas have the columns, order and types of the Spark engine") {
    assert(AdbcMetadataSchemas.Columns.map(_.name) === List(
      "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
      "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
      "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
      "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE",
      "IS_AUTO_INCREMENT"))
    val types = AdbcMetadataSchemas.Columns.map(c => c.name -> c.tTypeId).toMap
    assert(types("DATA_TYPE") === TTypeId.INT_TYPE)
    assert(types("BUFFER_LENGTH") === TTypeId.TINYINT_TYPE)
    assert(types("SOURCE_DATA_TYPE") === TTypeId.SMALLINT_TYPE)
    assert(types("IS_NULLABLE") === TTypeId.STRING_TYPE)
    assert(AdbcMetadataSchemas.Schemas.map(_.name) === List("TABLE_SCHEM", "TABLE_CATALOG"))
    assert(AdbcMetadataSchemas.Tables.map(_.name) === List(
      "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
      "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"))
    assert(AdbcMetadataSchemas.CurrentDatabase.map(_.name) === List("TABLE_SCHEM"))
    assert(AdbcMetadataSchemas.CurrentCatalog.map(_.name) === List("TABLE_CAT"))
  }

  test("a GetColumns row set encodes, including the TINYINT and SMALLINT columns left null") {
    val row: Seq[Any] = Seq(
      "c", "s", "t", "a", Int.box(4), "INT", Int.box(10), null, Int.box(0), Int.box(10),
      Int.box(1), null, null, null, null, null, Int.box(1), "YES", null, null, null, null, "")
    val rowSet = AdbcTRowSetGenerator.toTRowSet(
      Seq(row), AdbcMetadataSchemas.Columns, org.apache.kyuubi.shaded.hive.service.rpc.thrift
        .TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10)
    assert(rowSet.getColumnsSize === 23)
    assert(rowSet.getColumns.get(7).isSetByteVal) // BUFFER_LENGTH
    assert(rowSet.getColumns.get(21).isSetI16Val) // SOURCE_DATA_TYPE
  }

  test("LIKE patterns: wildcards, escapes, and null, empty and * mean everything") {
    val m = LikePattern.matcher("my\\_db%")
    assert(m("my_db") && m("my_db2") && !m("myXdb"))
    assert(LikePattern.matcher("my_db")("myXdb")) // an unescaped _ is a pattern character
    assert(LikePattern.matcher("a.b")("a.b") && !LikePattern.matcher("a.b")("aXb"))
    Seq(null, "", "*", "%").foreach { p =>
      assert(LikePattern.matcher(p)("anything") && LikePattern.matcher(p)(null))
    }
    assert(!LikePattern.matcher("a%")(null))
    assert(LikePattern.exact("my_db")("my_db") && !LikePattern.exact("my_db")("myXdb"))
    assert(LikePattern.exact(null)("x") && LikePattern.exact("")("x"))
  }

  test("what is handed to the driver: no backslash patterns, no catalog with wildcards") {
    assert(LikePattern.forDriver("ord%") === "ord%")
    assert(LikePattern.forDriver("a\\_b") === null)
    assert(LikePattern.forDriver(null) === null && LikePattern.forDriver("%") === null)
    assert(LikePattern.catalogForDriver("memory") === "memory")
    assert(LikePattern.catalogForDriver("my_db") === null) // would also match myXdb
    assert(LikePattern.catalogForDriver(null) === null)
  }

  test("getObjects output is read from real vectors, with absent fields as None") {
    val allocator = new RootAllocator(Long.MaxValue)
    var root: VectorSchemaRoot = null
    var reader: ArrowStreamReader = null
    try {
      root = VectorSchemaRoot.create(StandardSchemas.GET_OBJECTS_SCHEMA, allocator)
      root.allocateNew()
      root.getVector("catalog_name").asInstanceOf[VarCharVector]
        .setSafe(0, "memory".getBytes("UTF-8"))
      val w = root.getVector("catalog_db_schemas").asInstanceOf[ListVector].getWriter
      w.setPosition(0)
      w.startList()
      val s = w.struct()
      s.start()
      s.varChar("db_schema_name").writeVarChar(new Text("main"))
      val tables = s.list("db_schema_tables")
      tables.startList()
      val t = tables.struct()
      t.start()
      t.varChar("table_name").writeVarChar(new Text("t"))
      t.varChar("table_type").writeVarChar(new Text("BASE TABLE"))
      val cols = t.list("table_columns")
      cols.startList()
      val c = cols.struct()
      c.start()
      c.varChar("column_name").writeVarChar(new Text("a"))
      c.integer("ordinal_position").writeInt(1)
      c.varChar("xdbc_type_name").writeVarChar(new Text("INTEGER"))
      c.smallInt("xdbc_data_type").writeSmallInt(4.toShort)
      c.end()
      cols.endList()
      t.end()
      tables.endList()
      s.end()
      w.endList()
      root.setRowCount(1)
      root.getFieldVectors.asScala.foreach(_.setValueCount(1))

      val bytes = new ByteArrayOutputStream()
      val writer = new ArrowStreamWriter(root, null, bytes)
      writer.start()
      writer.writeBatch()
      writer.end()
      reader = new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray), allocator)

      assert(AdbcObjects.read(reader) === Seq(
        ObjectCatalog(
          Some("memory"),
          Seq(ObjectSchema(
            Some("main"),
            Seq(ObjectTable(
              "t",
              Some("BASE TABLE"),
              Seq(ObjectColumn(
                "a", Some(1), None, Some("INTEGER"), Some(4), None, None, None, None, None,
                None)))))))))
    } finally {
      if (reader != null) reader.close()
      if (root != null) root.close()
      allocator.close()
    }
  }

  test("GetCatalogs lists distinct non-null catalog names") {
    val source = new FakeSource(warehouse :+ ObjectCatalog(None, Nil) :+ warehouse.head)
    assert(AdbcMetadataQueries.catalogs(source) ===
      Seq(Seq("my_db"), Seq("myXdb"), Seq("system")))
  }

  test("GetSchemas filters by exact catalog and schema pattern even if the driver does not") {
    val source = new FakeSource(warehouse)
    // my_db contains _, so it is not handed to the driver (it would also match myXdb)
    assert(AdbcMetadataQueries.schemas(source, "my_db", "%") ===
      Seq(Seq("main", "my_db"), Seq("raw_data", "my_db")))
    assert(source.asked.head._1 === null)
    assert(AdbcMetadataQueries.schemas(source, null, "raw\\_%") === Seq(Seq("raw_data", "my_db")))
    assert(AdbcMetadataQueries.schemas(source, "system", null) === Nil)
    assert(AdbcMetadataQueries.schemas(source, "myXdb", "main") === Seq(Seq("main", "myXdb")))
  }

  test("GetTables normalizes table types and filters by type, schema and table") {
    val source = new FakeSource(warehouse)
    def names(rows: Seq[Seq[Any]]) = rows.map(r => (r(1), r(2), r(3)))
    assert(names(AdbcMetadataQueries.tables(source, "my_db", "main", "%", null)) ===
      Seq(("main", "orders", "TABLE"), ("main", "orders_v", "VIEW")))
    assert(names(AdbcMetadataQueries.tables(source, "my_db", null, null, Seq("VIEW"))) ===
      Seq(("main", "orders_v", "VIEW")))
    val both = Seq("TABLE", "VIEW")
    assert(names(AdbcMetadataQueries.tables(source, "my_db", null, "ord_rs", both)) ===
      Seq(("main", "orders", "TABLE")))
    // the driver's own name for a plain table is accepted as well
    val raw = AdbcMetadataQueries.tables(source, "my_db", "raw\\_data", null, Seq("BASE TABLE"))
    assert(names(raw) === Seq(("raw_data", "events", "TABLE")))
    val row = AdbcMetadataQueries.tables(source, "myXdb", null, null, null).head
    assert(row.size === AdbcMetadataSchemas.Tables.size)
    assert(row.take(4) === Seq("myXdb", "main", "other", "TABLE") && row.drop(4).forall(_ == null))
  }

  test("GetTableTypes are normalized, distinct and sorted") {
    val source = new FakeSource(Nil, types = Seq("VIEW", "BASE TABLE", "TABLE", "LOCAL TEMPORARY"))
    assert(AdbcMetadataQueries.tableTypes(source) ===
      Seq(Seq("LOCAL TEMPORARY"), Seq("TABLE"), Seq("VIEW")))
  }

  private def schemaOf(fields: Field*): Schema = new Schema(fields.asJava)

  private def columnValue(row: Seq[Any], name: String): Any =
    row(AdbcMetadataSchemas.Columns.indexWhere(_.name == name))

  test("GetColumns types come from the table's Arrow schema, as a SELECT advertises them") {
    val catalogs = Seq(ObjectCatalog(Some("c"), Seq(ObjectSchema(Some("s"), Seq(
      ObjectTable("t", Some("BASE TABLE"), Seq(
        column("id", 1, "BIGINT"), column("price", 2), column("name", 3), column("tags", 4))))))))
    val tableSchema = schemaOf(
      field("id", new ArrowType.Int(64, true), nullable = false),
      field("price", ArrowType.Decimal.createDecimal(10, 2, 128)),
      field("name", ArrowType.Utf8.INSTANCE),
      new Field(
        "tags",
        new FieldType(true, ArrowType.List.INSTANCE, null),
        java.util.Arrays.asList(field("item", ArrowType.Utf8.INSTANCE))))
    val source = new FakeSource(catalogs, Map("t" -> tableSchema))
    val rows = AdbcMetadataQueries.columns(source, null, null, null, null)
    assert(rows.size === 4)
    rows.foreach(r => assert(r.size === AdbcMetadataSchemas.Columns.size))
    def at(row: Seq[Any], name: String) = columnValue(row, name)

    val id = rows(0)
    assert(Seq("c", "s", "t", "id").zip(id).forall { case (a, b) => a == b })
    assert(at(id, "DATA_TYPE") === Types.BIGINT)
    assert(at(id, "TYPE_NAME") === "BIGINT") // the driver's name wins
    assert(at(id, "COLUMN_SIZE") === 19 && at(id, "DECIMAL_DIGITS") === 0)
    assert(at(id, "NULLABLE") === 0 && at(id, "IS_NULLABLE") === "NO")
    assert(at(id, "ORDINAL_POSITION") === 1)

    val price = rows(1)
    assert(at(price, "DATA_TYPE") === Types.DECIMAL && at(price, "TYPE_NAME") === "DECIMAL")
    assert(at(price, "COLUMN_SIZE") === 10 && at(price, "DECIMAL_DIGITS") === 2)
    assert(at(price, "NULLABLE") === 1 && at(price, "IS_NULLABLE") === "YES")

    // advertised as text in result sets, so text here too
    assert(at(rows(2), "DATA_TYPE") === Types.VARCHAR)
    assert(at(rows(3), "DATA_TYPE") === Types.VARCHAR)
    assert(at(id, "BUFFER_LENGTH") == null && at(id, "SOURCE_DATA_TYPE") == null)
  }

  test("the JDBC type of a column is derived from the type the engine advertises") {
    def jdbc(t: ArrowType) = JdbcTypes.of(AdbcSchema.column(field("x", t)))
    // TINYINT is sent as SMALLINT
    assert(jdbc(new ArrowType.Int(8, true)).dataType === Types.SMALLINT)
    assert(jdbc(new ArrowType.Int(32, false)).dataType === Types.BIGINT) // UINTEGER widens
    assert(jdbc(new ArrowType.Int(64, false)).dataType === Types.VARCHAR) // UBIGINT is text
    val single = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    assert(jdbc(single).dataType === Types.FLOAT)
    assert(jdbc(ArrowType.Bool.INSTANCE).dataType === Types.BOOLEAN)
    assert(jdbc(ArrowType.Decimal.createDecimal(40, 2, 256)).dataType === Types.VARCHAR)
    assert(jdbc(new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.NANOSECOND, null))
      .dataType === Types.VARCHAR)
    assert(jdbc(new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null))
      .dataType === Types.TIMESTAMP)
  }

  test("GetColumns falls back to the driver's xdbc fields and reports a schema that fails") {
    val xdbc = ObjectColumn(
      "n", None, Some("a note"), Some("NUMERIC"), Some(Types.NUMERIC), Some(12), Some(3), Some(10),
      Some(1), Some("0"), Some(true))
    val catalogs = Seq(ObjectCatalog(Some("c"), Seq(ObjectSchema(Some("s"), Seq(
      ObjectTable("broken", Some("TABLE"), Seq(xdbc)),
      ObjectTable("noschema", Some("TABLE"), Seq(column("z", 5))))))))
    val source = new FakeSource(catalogs, failSchemaFor = Set("broken"))
    var reported: Seq[String] = Nil
    val rows = AdbcMetadataQueries.columns(
      source, null, null, null, null, (t, _) => reported = reported :+ t.name)
    assert(reported === Seq("broken"))
    def at(row: Seq[Any], name: String) = columnValue(row, name)
    val n = rows(0)
    assert(at(n, "DATA_TYPE") === Types.NUMERIC && at(n, "TYPE_NAME") === "NUMERIC")
    assert(at(n, "COLUMN_SIZE") === 12 && at(n, "DECIMAL_DIGITS") === 3)
    assert(at(n, "NULLABLE") === 1 && at(n, "IS_NULLABLE") === "YES")
    assert(at(n, "REMARKS") === "a note" && at(n, "COLUMN_DEF") === "0")
    assert(at(n, "ORDINAL_POSITION") === 1 && at(n, "IS_AUTO_INCREMENT") === "YES")
    // the driver gave nothing at all here: unknown nullability, text, ordinal from the order
    val z = rows(1)
    assert(at(z, "DATA_TYPE") === Types.VARCHAR && at(z, "NULLABLE") === 2)
    assert(at(z, "IS_NULLABLE") === "" && at(z, "ORDINAL_POSITION") === 5)
  }

  test("GetColumns filters tables and columns by pattern, and asks the schema only when needed") {
    val catalogs = Seq(ObjectCatalog(Some("c"), Seq(ObjectSchema(Some("s"), Seq(
      ObjectTable("t1", Some("TABLE"), Seq(column("id", 1), column("name", 2))),
      ObjectTable("t2", Some("TABLE"), Seq(column("id", 1))),
      ObjectTable("other", Some("TABLE"), Seq(column("id", 1))))))))
    val source = new FakeSource(catalogs)
    val rows = AdbcMetadataQueries.columns(source, "c", "s", "t_", "id")
    assert(rows.map(r => (r(2), r(3))) === Seq(("t1", "id"), ("t2", "id")))
    assert(source.schemaCalls === Seq("t1", "t2"))
    assert(AdbcMetadataQueries.columns(source, "c", "s", "t1", "nomatch%") === Nil)
    assert(source.schemaCalls === Seq("t1", "t2")) // no columns wanted, no schema call
  }
}
