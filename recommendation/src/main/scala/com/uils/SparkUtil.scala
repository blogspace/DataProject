package com.uils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @@function spark通用工具类
  */
object SparkUtil {
  /**
    * @function 创建SparkContext对象(添加日志过滤)
    * @param name：appname
    * @param master:local or cluster
    * @return SparkContext对象sc
    */
  def createSparkContext(name:String)={
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName(name).setMaster("local")
    val sc = new SparkContext(conf)
    sc
  }

  def dataSplit(line:String)={
    val col = line.split("\t",-1)
    col
  }

  /**
    * @function
    * @param sc
    * @param path
    * @return
    */
  def fileSplit(sc:SparkContext,path:String)={
    val dataInput= sc.textFile(path).mapPartitions(iter=>{
     iter.map(line=>{
       val col = line.split(",",-1)
       col
     })
    })
    dataInput
  }

  /**
    * @function 获取现在的时间
    * @return 当前年月日
    */
  def nowDate()={
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = df.format(new Date())
    time
  }
  /**
    * @function 获取前一天的日期
    * @return 返回前一天的日期
    */
  def lastDate():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -365)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
}
