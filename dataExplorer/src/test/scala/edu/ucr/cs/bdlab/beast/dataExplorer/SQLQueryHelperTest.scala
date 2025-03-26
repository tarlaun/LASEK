package edu.ucr.cs.bdlab.beast.dataExplorer

import edu.ucr.cs.bdlab.test.BeastSpatialTest
import org.apache.calcite.sql.parser.SqlParseException
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SQLQueryHelperTest extends AnyFunSuite with BeastSpatialTest {
  test("SQLQueryProcessor should correctly identify a valid SQL query") {
    val validSQL = "SELECT * FROM users"
    SQLQueryHelper.extractTables(validSQL)
  }

  test("SQLQueryProcessor should identify an invalid SQL query") {
    val invalidSQL = "SELEC * FROM users"
    intercept[SqlParseException] {
      SQLQueryHelper.extractTables(invalidSQL)
    }
  }

  test("SQLQueryProcessor should extract table names from a valid SQL query with Join") {
    val sqlWithMultipleTables = "SELECT a.name, b.age FROM users a JOIN orders b ON a.id = b.user_id"
    val expectedTables = Set("USERS", "ORDERS")
    val result = SQLQueryHelper.extractTables(sqlWithMultipleTables)
    assertResult(expectedTables)(result)
  }

  test("SQLQueryProcessor should extract table names from a valid SQL query with single table") {
    val sqlWithMultipleTables = "SELECT a.name FROM users a"
    val expectedTables = Set("USERS")
    val result = SQLQueryHelper.extractTables(sqlWithMultipleTables)
    assertResult(expectedTables)(result)
  }

  test("SQLQueryProcessor should extract table names from a valid SQL query without alias") {
    val sqlWithMultipleTables = "SELECT users.name FROM users"
    val expectedTables = Set("USERS")
    val result = SQLQueryHelper.extractTables(sqlWithMultipleTables)
    assertResult(expectedTables)(result)
  }

  test("SQLQueryProcessor should return empty set for a query without tables") {
    val sqlWithoutTables = "SELECT CURRENT_TIMESTAMP"
    val expectedTables = Set.empty[String]
    val result = SQLQueryHelper.extractTables(sqlWithoutTables)
    assertResult(expectedTables)(result)
  }

  test("Extract table names from a query with subqueries") {
    val sql = "SELECT * FROM (SELECT * FROM users) u INNER JOIN orders o ON u.id = o.user_id"
    val expectedTables = Set("USERS", "ORDERS")
    assertResult(expectedTables)(SQLQueryHelper.extractTables(sql))
  }

  test("Extract table names from a query with CTEs") {
    val sql = "WITH cte AS (SELECT * FROM users) SELECT * FROM cte INNER JOIN orders ON cte.id = orders.user_id"
    val expectedTables = Set("USERS", "ORDERS")
    assertResult(expectedTables)(SQLQueryHelper.extractTables(sql))
  }

  test("Extract table names from a query with UNION") {
    val sql = "SELECT name FROM users UNION SELECT name FROM managers"
    val expectedTables = Set("USERS", "MANAGERS")
    assertResult(expectedTables)(SQLQueryHelper.extractTables(sql))
  }
}
