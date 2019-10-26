package com.demo

import java.sql.DriverManager
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util
import java.util.Hashtable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.
{RichSourceFunction, SourceFunction}
import scala.collection.mutable

case class RegionInfo(region: String, value: String, inner_code: String)

class JdbcReader extends RichSourceFunction[RegionInfo] {
  private var connection: Connection = null
  private var ps: PreparedStatement = null
  private var isRunning: Boolean = true

  override def cancel(): Unit = {
    super.close()
    if (connection != null) {
      connection.close()
    }
    if (ps != null) {
      ps.close()
    }
    isRunning = false
  }

  override def run(ctx: SourceFunction.SourceContext[RegionInfo]): Unit = {
    while (isRunning) {
      val resultSet = ps.executeQuery()
      while (resultSet.next()) {
        val rangeInfo = RegionInfo(resultSet.getString("region"), resultSet.getString("value"), resultSet.getString("inner_code"))ctx.collect(rangeInfo)
      }
      //休息2分钟
      Thread.sleep(5000 * 60)
    }
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val driverClass = "com.mysql.jdbc.Driver"
    val dbUrl = "jdbc:mysql://localhost:3306/flink"
    val userName = "root"
    val passWord = "1234"
    connection = DriverManager.getConnection(dbUrl, userName, passWord)
    ps = connection.prepareStatement("select region, value, inner_code fromevent_mapping")
  }
}
