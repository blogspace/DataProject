package com.service

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}

//    timestr(string) ,offsetNum(int),app_user_id(string) ,vid(string) ,time(string) ,fromtype (string) ,url (string) ,referrer_url (string),event (string) ,type (string) ,app_id (string) ,channel_id (string) ,device_id (string) ,extra_id (string)
object DemoTest {

  case class table(timestr: String, offsetNum: Int, app_user_id: String, vid: String, time: String, fromtype: String, url: String, referrer_url: String, event: String, typed: String, app_id: String, channel_id: String, device_id: String, extra_id: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("tset").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //    import sqlContext.implicits._
    //    val result = sc.textFile("D:\\admin\\Desktop\\log").map(_.split("\t", -1))
    //      .map(line => {
    //        table(line(0), line(1).toInt, line(2), line(3), line(4), line(5), line(6), line(7), line(8), line(9), line(10), line(11), line(12), line(13))
    //      }).toDF()

    val config: Config = ConfigFactory.load("my.properties")
    println(config.getString("jdbc.url.test"))
    Dri

    //    val sql = "insert into log(timestr,offsetNum,app_user_id,vid,time,fromtype,url,referrer_url,event,typed,app_id,channel_id,device_id,extra_id) " +
    //      "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    //    result.foreachPartition(partition => saveToMysql(partition, properties, sql))
    //loadDB(sqlContext,properties).map(line=>line.getAs[String]("timestr")+" "+line.getAs[Int](1)).show(30)
    //    val partitons = new Array[String](6)
    //    partitons(0) = "1=1 limit 0,10000"
    //    partitons(1) = "1=1 limit 10000,10000"
    //    partitons(2) = "1=1 limit 20000,10000"
    //    partitons(3) = "1=1 limit 30000,10000"
    //    partitons(4) = "1=1 limit 40000,10000"
    //    partitons(5) = "1=1 limit 50000,10000"
    //    val total = loadDB(sqlContext, properties,partitons).count()
    //    println(total)

  }


  def saveDB(data: DataFrame, prop: Properties) = {
    val pro = new Properties()
    pro.setProperty("user", prop.getProperty("jdbc.user"))
    pro.setProperty("password", prop.getProperty("jdbc.password"))
    data.write.mode(SaveMode.Overwrite).jdbc(prop.getProperty("jdbc.url"), prop.getProperty("jdbc.table"), pro)
  }

  def loadDB(sqlContext: SQLContext, prop: Properties) = {
    sqlContext.read.format("jdbc").option("url", prop.getProperty("jdbc.url"))
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", prop.getProperty("jdbc.table"))
      .option("user", prop.getProperty("jdbc.user"))
      .option("password", prop.getProperty("jdbc.password"))
      .load()
  }

  def loadDB(sqlContext: SQLContext, prop: Properties, predicates: Array[String]) = {
    val pro = new Properties()
    pro.setProperty("user", prop.getProperty("jdbc.user"))
    pro.setProperty("password", prop.getProperty("jdbc.password"))
    sqlContext.read.jdbc(prop.getProperty("jdbc.url"), prop.getProperty("jdbc.table"), predicates, pro)
  }

  def loadDB(sqlContext: SQLContext, prop: Properties, column: String) = {
    sqlContext.read.format("jdbc").option("url", prop.getProperty("jdbc.url"))
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", prop.getProperty("jdbc.table"))
      .option("user", prop.getProperty("jdbc.user"))
      .option("password", prop.getProperty("jdbc.password"))
      .option("partitionColumn", column)
      .option("lowerBound", 1)
      .option("upperBound", 10000)
      .option("numPartitions", 5)
      .option("fetchsize", 100)
      .load()
  }

  //(String, Int,String,String,String,String,String,String,String,String,String,String,String,String)
  def saveToMysql(partition: Iterator[Row], prop: Properties, sql: String): Unit = {
    var connect: Connection = null
    var pstmt: PreparedStatement = null
    Class.forName("com.mysql.jdbc.Driver")
    connect = DriverManager.getConnection(prop.getProperty("jdbc.url"), prop.getProperty("jdbc.user"), prop.getProperty("jdbc.password"))
    connect.setAutoCommit(false)
    pstmt = connect.prepareStatement(sql)
    partition.foreach(line => {
      pstmt.setString(1, line.getString(0))
      pstmt.setInt(2, line.getInt(1))
      pstmt.setString(3, line.getString(2))
      pstmt.setString(4, line.getString(3))
      pstmt.setString(5, line.getString(4))
      pstmt.setString(6, line.getString(5))
      pstmt.setString(7, line.getString(6))
      pstmt.setString(8, line.getString(7))
      pstmt.setString(9, line.getString(8))
      pstmt.setString(10, line.getString(9))
      pstmt.setString(11, line.getString(10))
      pstmt.setString(12, line.getString(11))
      pstmt.setString(13, line.getString(12))
      pstmt.setString(14, line.getString(13))
      pstmt.addBatch()

    })
    pstmt.executeBatch()

    connect.close()
    pstmt.close()
  }

  def properties(): Properties = {
    val props = new Properties()
    val in = this.getClass.getClassLoader.getResourceAsStream("my.properties")
    props.load(in)
    props
  }
}
