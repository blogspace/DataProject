package com.jianbing.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.Seconds

/**
  * @function spark驱动
  * @author create by liuhao at 2019/6/17 11:42
  */
object DriverFactory {
  private var sc: SparkContext = null
  private var ssc: StreamingContext = null
  private var sparkSql: SQLContext = null
  private var spark: SparkSession = null

  /**
    * @function sparkContext对象
    * @param name
    * @return
    */
  def sparkContext(name: String) = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName(name).setMaster("local[*]")
    sc = new SparkContext(conf)
    sc
  }

  /**
    * @function streamingContext对象
    * @param x
    * @param y
    */
//  def streamingContext(x: SparkContext, y: Int) = {
//    ssc = new StreamingContext(x, Seconds(y))
//    ssc
//  }

  /**
    * @functtion sqlContext对象
    * @param x
    */
  def sqlContext(x: SparkContext) = {
    sparkSql = new SQLContext(x)
    sparkSql
  }

  /**
    * @function sparkSession对象
    * @param x
    * @param y
    * @return
    */
  def sparkSession(x: String, y: String) = {
    spark = SparkSession.builder().appName(x).master(y).getOrCreate()
    spark
  }


}
