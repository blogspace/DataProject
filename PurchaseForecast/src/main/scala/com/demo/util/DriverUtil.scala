package com.demo.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @function
  * @author create by liuhao at 2019/6/20 14:55
  */
object DriverUtil {
  private var sc: SparkContext = null

  /**
    * @function 创建SparkContext对象
    * @param name   appname
    * @param master local or cluster
    * @return SparkContext对象sc
    */
  def sparkContext(appName: String, master: String) = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    sc = new SparkContext(conf)
    sc
  }
}
