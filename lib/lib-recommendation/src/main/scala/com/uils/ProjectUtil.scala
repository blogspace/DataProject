package com.uils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * @function pro_dwd_reg_info工具类
  * @create by liuhao at 2019/5/14
  */
object ProjectUtil {
  case class rating(userID: String, movieID: Int, rating: Double)
  case class recommendation(userID:String,movie1:String,movie2:String,movie3:String,movie4:String,movie5:String)
  /**
    * @funcion 评分缺失值处理，-1代表没有评论
    * @param rating
    * @return
    */
  def MissingValue(rating:String)=rating match {
    case "-1" => 3.toDouble
    case _ =>rating.toDouble
  }

}
