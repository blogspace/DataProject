package com.jianbing.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @function
  * @author create by liuhao at 2019/8/6 16:24
  */
object Common {
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

  def distinct(line: RDD[(String, Array[String])]) = {
    line.map(x => (x._1, x._2.mkString("\001"))).aggregateByKey(Set[String]())(
      (set, fields) => set + fields, (part1, part2) => {
        part1 ++ part2
      }).map(line => line._2.map(x => (line._1, x)))
  }

}
