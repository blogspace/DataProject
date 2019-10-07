package com.demo.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @@function spark通用工具类
  */
object SparkUtil {
  /**
    * @function 数据切割
    * @param line
    * @return
    */
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
    val dataInput= sc.textFile(path).map(line=>{
      val col = line.split("\t",-1)
      col
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
