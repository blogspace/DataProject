package com.uils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @function pro_dwd_reg_info驱动工具类
  * @create by liuhao at 2019/5/14
  */
trait AppConf {
  val conf = new SparkConf().setAppName("recommendation").setMaster("local[*]")
  val sc = new SparkContext(conf)
 // sc.setLogLevel("error")
}
