package com.jianbing.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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




}
