package com.jianbing.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}


object DataUtil {
  /**
    * 1-"yyyy-MM-dd" 2-"yyyyMMdd" 3-"yyyy/MM/dd" 4-"yyyy-MM-dd HH:mm:ss" 5-"yyyy" 6-"HH"
    */

  /**n
    * @function 获取现在的时间
    * @return 当前年月日
    */
  def nowDate(dataFormat: String) = {
    val df = new SimpleDateFormat(dataFormat)
    val time = df.format(new Date())
    time
  }

  /**
    * @function 获取过去的时间
    * @return 返回前天的日期
    */
  def lastDate(dataFormat: String, n: Int): String = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat(dataFormat)
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -n)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  /**
    * @function 将某种格式的时间转换为另一种格式的时间
    * @param date
    * @param srcDataFormat
    * @param tarDataFormat
    * @return
    */
  def dateTransform(date: String, srcDataFormat: String, tarDataFormat: String) = {
    val time: Date = new SimpleDateFormat(srcDataFormat).parse(date)
    var ymd = new SimpleDateFormat(tarDataFormat).format(time)
    ymd
  }

  /**
    * @function 将时间转为毫秒
    * @param srcDate
    * @param srcDateFormat
    * @return
    */
  def dateToSeconds(srcDate: String, srcDateFormat: String) = {
    val date: Date = new SimpleDateFormat(srcDateFormat).parse(srcDate)
    date.getTime
  }

  /**
    * @function 将毫秒转为目标时间
    * @param srcSeconds
    * @param tarDateFormat
    * @return
    */
  def secondsToDate(srcSeconds: String, tarDateFormat: String) = {
    new SimpleDateFormat(tarDateFormat).format(srcSeconds)
  }

  /**
    * @function 将时间转为毫秒
    * @param srcDate
    * @param srcDateFormat
    * @return
    */
  def dateToSeconds(srcDate: String) = {
    val dateFormat = "yyyy-MM-dd HH:mm:ss"
    val date: Date = new SimpleDateFormat(dateFormat).parse(srcDate)
    date.getTime
  }

  /**
    * @function 计算时间差
    * @param fromDate
    * @param toDate
    * @return
    */
  def dateDiff(fromDate: String, toDate: String) = {
    try {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val fd = df.parse(fromDate)
      val td = df.parse(toDate)
      //val day = (td.getTime - fd.getTime) / 86400000
      //day.toDouble
      val second = (td.getTime - fd.getTime) / 1000
      second.toInt
    } catch {
      case e: Exception => 0
    }
  }

  /**
    * @function 最小时间
    * @param x
    * @param y
    * @return
    */
  def minTime(x: String, y: String) = {
    if (isNormalDate(x) == true && isNormalDate(y) == true) {
      if (dateDiff(x, y) >= 0) x else y
    } else if(isNormalDate(x) == false) {
      y
    }else{
      x
    }
  }

  /**
    * @function 判断时间格式
    * @param tm
    * @param mode
    * @return
    */
  def isNormalDate(tm: String, mode: String = "yyyy-MM-dd HH:mm:ss"): Boolean = {
    var date: Date = null
    try {
      date = new SimpleDateFormat(mode).parse(tm)
      true
    } catch {
      case _: Exception => false // 默认给1800-01-01 00:00:00
    }
  }


}
