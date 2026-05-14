package example

import java.nio.file.Files
import java.sql.{Connection, DriverManager}
import scala.collection.mutable

/** One in-process analytical shard. Each session owns its own
  * in-memory DuckDB connection on a pinned thread.
  *
  * No data is loaded at startup — the shard is empty until callers
  * upload a CSV via `loadCsv`. Sticky routing on the pool means
  * "your data lives where you put it": load with key `K`, query
  * later with key `K` and you hit the same shard.
  *
  * The pinned-thread requirement here is JDBC-shaped rather than
  * crash-loud: a `Connection` with an open transaction or live
  * prepared-statement state isn't safe to hand across threads. The
  * pool gives each connection a single owning thread for its
  * lifetime, which is exactly what JDBC wants. Per-connection
  * `PRAGMA threads=2` keeps DuckDB's intra-query worker pool
  * bounded so N shards × M threads doesn't oversubscribe the host.
  */
final class DuckDbShardResource(val partitionId: Int) extends AutoCloseable {

  private val conn: Connection = DriverManager.getConnection("jdbc:duckdb:")

  locally {
    val s = conn.createStatement()
    try s.execute("PRAGMA threads=2") finally s.close()
  }

  /** Replace `<table>` with the contents of the uploaded CSV. The
    * actor writes the bytes to a temp file, hands the path to
    * DuckDB's `read_csv_auto` (which infers the schema), then
    * cleans the file up. Returns the loaded row count.
    *
    * `table` is validated as a SQL identifier — the rest of the
    * statement is composed; bytes never reach SQL parsing. */
  def loadCsv(table: String, csv: Array[Byte]): Long = {
    requireValidIdentifier(table)
    val tmp = Files.createTempFile("upload-", ".csv")
    try {
      Files.write(tmp, csv)
      val s = conn.createStatement()
      try s.execute(
        s"CREATE OR REPLACE TABLE $table AS " +
          s"SELECT * FROM read_csv_auto('${tmp.toAbsolutePath}', header=true)",
      ) finally s.close()
      count(table)
    } finally Files.deleteIfExists(tmp)
  }

  /** Run an arbitrary SELECT, return rows as plain maps. The
    * caller (HTTP layer) is responsible for serializing them. */
  def query(sql: String): List[Map[String, Any]] = {
    val s  = conn.createStatement()
    val rs = s.executeQuery(sql)
    try {
      val md   = rs.getMetaData
      val cols = (1 to md.getColumnCount).map(md.getColumnLabel).toVector
      val out  = mutable.ListBuffer.empty[Map[String, Any]]
      while (rs.next()) {
        val row = mutable.LinkedHashMap.empty[String, Any]
        cols.foreach(c => row(c) = rs.getObject(c))
        out += row.toMap
      }
      out.toList
    } finally {
      rs.close()
      s.close()
    }
  }

  /** Tables present in the `main` schema with their row counts. */
  def tables(): List[(String, Long)] = {
    val s  = conn.createStatement()
    val rs = s.executeQuery(
      "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name",
    )
    val names = mutable.ListBuffer.empty[String]
    try while (rs.next()) names += rs.getString(1)
    finally { rs.close(); s.close() }
    names.toList.map(n => n -> count(n))
  }

  def hasTable(name: String): Boolean = {
    val ps = conn.prepareStatement(
      "SELECT 1 FROM information_schema.tables WHERE table_schema='main' AND table_name = ?",
    )
    ps.setString(1, name)
    val rs = ps.executeQuery()
    try rs.next()
    finally { rs.close(); ps.close() }
  }

  /** Row count of the named table (caller must have validated the
    * identifier OR be using a known-trusted name). */
  def count(table: String): Long = {
    requireValidIdentifier(table)
    val s  = conn.createStatement()
    val rs = s.executeQuery(s"SELECT COUNT(*) FROM $table")
    try { rs.next(); rs.getLong(1) }
    finally { rs.close(); s.close() }
  }

  override def close(): Unit = conn.close()

  private def requireValidIdentifier(name: String): Unit =
    if (!DuckDbShardResource.isValidIdentifier(name))
      throw new IllegalArgumentException(s"invalid SQL identifier: '$name'")
}

object DuckDbShardResource {
  /** SQL identifier shape: starts with letter/underscore, then
    * alphanumerics/underscores, capped at 64 chars. Whitelisted to
    * keep `table`/`col` route parameters from reaching SQL parsing
    * untrusted. Used by both the resource and the HTTP layer. */
  private val ValidIdentifier = "^[A-Za-z_][A-Za-z0-9_]{0,63}$".r

  def isValidIdentifier(s: String): Boolean = ValidIdentifier.matches(s)
}
