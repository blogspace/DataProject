package com.service

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.sql.Connection
import java.sql.DriverManager

import org.apache.spark.sql.SparkSession

object DemoTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("tset")//.setMaster("local[2]")
    val sc = new SparkContext(conf)
//    val spark = SparkSession.builder().appName("spark-hive").master("local")
//      .config("spark.sql.warehouse .dir", "hdfs://datanode:9000/hive/warehouse")
//      .enableHiveSupport()
//      .getOrCreate()
//    spark.sql("create table log.logtest like log.logclean")
//    spark.sql("insert into table log.logtest select * from log.logclean")
//    spark.sql("show tables")
//    spark.sql("select * from log.logtest")

  }

  def insertToMysql(iterator: Iterator[(String, Int)]): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.10.58:3306/test"
    val username = "root"
    val password = "1"
    var connectionMqcrm: Connection = null
    Class.forName(driver)
    connectionMqcrm = DriverManager.getConnection(url, username, password)
    val sql = "INSERT INTO t_spark (`name`,`num`) VALUES (?,?)"
    iterator.foreach(data => {
      val statement = connectionMqcrm.prepareStatement(sql)
      statement.setString(1, data._1)
      statement.setInt(2, data._2)
      var result = statement.executeUpdate()
      if (result == 1) {
        println("写入mysql成功.............")
      }
    })
  }
}
