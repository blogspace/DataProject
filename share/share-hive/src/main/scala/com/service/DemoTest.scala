package com.service

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DemoTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.jetty.server").setLevel(Level.OFF)
    val spark = SparkSession.builder().enableHiveSupport().appName("hive").master("local")
      .config("spark.sql.warehouse .dir", "hdfs://datanode:9000/hive/warehouse")
      .getOrCreate()
//        val table = "create table if not exists log.logClean(timestr string,offsetNum string,app_user_id string,vid string,time string,fromtype string,url string,referrer_url string,event string,type string,app_id string,channel_id string,device_id string,extra_id string)" +
//          "COMMENT 'Web Access Log'" +
//          "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'" +
//          "STORED AS TEXTFILE"
//    spark.sql("create database if not exists log")
//    spark.sql("show databases").show()
//    spark.sql("drop table if exists logClean ")
//    spark.sql(table)
//    spark.sql("LOAD DATA INPATH 'hdfs://datanode:9000/log' INTO TABLE log.logClean")
    //"LOAD DATA LOCAL INPATH 'D:\admin\Desktop\log' INTO TABLE log.logClean"
    //        spark.sql("drop table if exists log.logClean")
     spark.sql("select * from log.logClean").show()

  }

}
