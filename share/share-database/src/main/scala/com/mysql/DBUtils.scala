package com.mysql

import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}


object DBUtils {
  /**
    * @function 加载数据库
    * @param sqlContext
    * @param url
    * @param table
    * @param user
    * @param password
    * @return
    */
  def loadDB(sqlContext: SQLContext, url: String, table: String, user: String, password: String) = {
    sqlContext.read.format("jdbc").option("url", url)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .load()
  }

  def loadDB(sqlContext: SQLContext, url: String, table: String, user: String, password: String, column: String, index1: Int, index2: Int, partitions: Int) = {
    val Url = url
    val tableName = table
    val columnName = column
    val lowerBound = index1 //1
    val upperBound = index2 //6000000
    val numPartitions = partitions //10
    val prop = new Properties
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    sqlContext.read.jdbc(Url, tableName, columnName, lowerBound, upperBound, numPartitions, prop)
  }

  /**
    * @function 写数据库
    * @param data
    * @param url
    * @param table
    * @param user
    * @param password
    */
  def saveDB(data: DataFrame, url: String, table: String, user: String, password: String) = {
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    data.write.mode(SaveMode.Append).jdbc(url, table, prop)
  }

  /**
    * @function 读取hbase数据
    * @param servers
    * @param sc
    * @param table
    */
  def loadHbase(servers: String, sc: SparkContext, table: String) = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", servers)
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("zookeeper.session.timeout", "6000000")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, table)
    val ratingsData = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    val hbaseRatings = ratingsData.map { case (_, res) =>
      val app_user_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("app_user_id")))
      val extra_id = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("extra_id")))
      val count = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("count")))
      Array(extra_id.toInt, count.toDouble).mkString("\t")
    }
  }

  /**
    * @function 写入sqlServer
    * @param dataFrame
    * @param table
    * @param prop
    */
  def saveSqlServer(dataFrame: DataFrame, table: String, prop: Properties) = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", prop.getProperty("url"))
      .option("dbtable", table)
      .option("user", prop.getProperty("user"))
      .option("password", prop.getProperty("password"))
      .save()
  }


}
