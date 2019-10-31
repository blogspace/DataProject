package com

import java.sql.DriverManager
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

object OffsetManager {
  //读取配置文件
  val config: Config = ConfigFactory.load()

  //数据库连接参数配置
  def getConn = {
    DriverManager.getConnection(
      config.getString("db.url"),
      config.getString("db.user"),
      config.getString("db.password")
    )
  }
  /*
  获取偏移量信息
   */
  def apply(groupId: String, topic: String) = {
    val conn = getConn
    val statement = conn.prepareStatement("select * from kafka_offset where groupId=? and topic=?")
    statement.setString(1, groupId)
    statement.setString(2, topic)
    val rs = statement.executeQuery()
    //注意导包
    import scala.collection.mutable._
    val offsetRange = Map[TopicPartition, Long]()
    while (rs.next()) {
      //讲获取的数据放到map中
      offsetRange += new TopicPartition(rs.getString("topic"), rs.getInt("partition")) -> rs.getLong("untilOffset")
    }
    rs.close()
    statement.close()
    conn.close()
    offsetRange
  }

  /*
  保存当前偏移量到数据库
   */
  def saveCurrentBatchOffset(groupId: String, offsetRange: Array[OffsetRange]) = {
    val conn = getConn
    val statement = conn.prepareStatement("replace into kafka_offset values(?,?,?,?)")
    for (i <- offsetRange) {
      statement.setString(1, i.topic)
      statement.setInt(2, i.partition)
      statement.setLong(3, i.untilOffset)
      statement.setString(4, groupId)
      statement.executeUpdate()

    }
    statement.close()
    conn.close()

  }
}
