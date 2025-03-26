package edu.ucr.cs.bdlab.beast.dataExplorer

import java.sql.Connection

object WorkspaceProcessor {
  def workspaceExists(workspaceId: Int, dbConnection: Connection): Boolean = {
    val statement = dbConnection.createStatement()

    try {
      val sqlQuery = s"SELECT COUNT(*) FROM workspaces WHERE id = $workspaceId"
      val resultSet = statement.executeQuery(sqlQuery)

      if (resultSet.next()) {
        resultSet.getInt(1) > 0
      } else {
        false
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        false
    } finally {
      if (statement != null) statement.close()
    }
  }
}
