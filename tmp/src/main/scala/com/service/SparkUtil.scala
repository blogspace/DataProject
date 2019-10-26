package com.service

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * @function spark工具类
  * @author create by liuhao at 2019/6/17 11:42
  */
object SparkUtil {
  /**
    * @function 对每一行数据切割
    * @param line 每一行数据
    * @return 数组
    */
  def dataSplit(line: String) = {
    line.split("\t", -1)
  }

  /**
    * @function 根据路径对每一行数据切割
    * @param sc   SparkContext
    * @param path 文件路径
    * @return 数组
    */
  def fileSplit(sc: SparkContext, path: String) = {
    sc.textFile(path).mapPartitions(partition => {
      partition.map(line => line match {
        case line if line.contains("\t") => line.split("\t", -1)
        case line if line.contains("\001") => line.split("\001", -1)
        case _ => line.split("\001", -1)
      })
    })
  }
  /**
    * @fucntion join操作
    * @param line
    * @param id
    * @return
    */
  def joinValue(line: (Array[String], Option[Array[String]]), id: Int) = {
    var array = new ArrayBuffer[String]()
    var arr = new Array[String](id)
    array ++= line._1
    if (line._2.size > 0) {
      line._2.foreach(line => {
        array ++= line
      })
    } else array ++= arr
    array.mkString("\001")
  }

  val regex1 = new Regex("null")
  val regex2 = new Regex("NULL")

  /**
    * @function null、NULL处理
    * @param x
    * @return
    */
  def nullValue(line:String)={
    line.replaceAll("null","").replaceAll("NULL","")
  }

  /**
    * @function 空值处理
    * @param x
    * @param y
    * @return
    */
  def ifNull(x: String, y: String) = {
    if (x.equals("null")) y else x
  }

  /**
    * @function 时间为空处理
    * @param date
    * @return
    */
  def ifNullTime(date: String) = {
    if (date.equals("null")) "2300-01-01" else date
  }





}
