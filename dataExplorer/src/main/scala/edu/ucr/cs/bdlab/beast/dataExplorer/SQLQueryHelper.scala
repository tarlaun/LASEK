package edu.ucr.cs.bdlab.beast.dataExplorer

import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.{SqlBasicCall, SqlCall, SqlIdentifier, SqlJoin, SqlKind, SqlNode, SqlNodeList, SqlSelect, SqlWith, SqlWithItem}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import org.apache.calcite.sql.util.{SqlBasicVisitor, SqlShuttle}
import org.apache.spark.internal.Logging

/**
 * Processes SQL queries to perform syntactic analysis and extract table names.
 */
object SQLQueryHelper {

  /**
   * Checks if an SQL query is syntactically correct and extracts table names from it.
   *
   * @param sql The SQL query string to be analyzed.
   * @return Either an error message as a string if the query is incorrect, or a set of table names if the query is correct.
   */
  def extractTables(sql: String): Set[String] = {
    val sqlNode = parseSQL(sql)
    extractTableNames(sqlNode)
  }

  /**
   * Parses the SQL query string into an AST (Abstract Syntax Tree) using Calcite's SQL parser.
   *
   * @param sql The SQL query string to be parsed.
   * @return The root node of the AST representing the parsed SQL query.
   */
  private def parseSQL(sql: String): SqlNode = {
    val parser = SqlParser.create(sql)
    parser.parseQuery()
  }

  /**
   * Extracts table names from the given AST node representing a SQL query.
   *
   * @param node The root node of the AST.
   * @return A set of table names extracted from the SQL query.
   */
  private def extractTableNames(node: SqlNode): Set[String] = {
    val tableNames = mutable.Set[String]()
    val visitor = new SqlIdentifierVisitor(tableNames)
    node.accept(visitor)
    tableNames.toSet
  }
}

class SqlIdentifierVisitor(tableNames: mutable.Set[String]) extends SqlShuttle with Logging {
  val cteNames: mutable.Set[String] = new mutable.HashSet[String]()

  override def visit(node: SqlIdentifier): SqlNode = {
    if (node.isSimple && !cteNames.contains(node.getSimple.toUpperCase)) {
      tableNames += node.getSimple
    }
    node
  }

  override def visit(call: SqlCall): SqlNode = {
    def visitNode(node: SqlNode): SqlNode = node match {
      case call: SqlCall => visit(call)
      case id: SqlIdentifier => visit(id)
      case list: SqlNodeList => visit(list)
      case other: SqlNode => throw new RuntimeException(s"Unsupported basic call '${other.getKind}'")
    }

    call match {
      case select: SqlSelect if select.getFrom == null => null
      case select: SqlSelect => visitNode(select.getFrom)
      case bc: SqlBasicCall if bc.getKind == SqlKind.AS => visitNode(bc.getOperandList.get(0))
      case withItem: SqlWithItem =>
        val cteName = withItem.getOperandList.get(0) match {
          case id: SqlIdentifier => id.getSimple
          case other => logWarning(s"Unsupported in CTE '${other.getKind}'"); null
        }
        if (cteName != null)
          cteNames.add(cteName.toUpperCase)
        visitNode(withItem.getOperandList.get(2))
      case _ =>
        super.visit(call)
    }
  }
}
