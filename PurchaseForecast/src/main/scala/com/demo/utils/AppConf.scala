package com.demo.utils

import org.apache.spark.SparkContext

trait AppConf {
  private var sparkContext:SparkContext = null
  def sc={
    sparkContext=SparkUtil.createSparkContext("pro_dwd_reg_info")
    sparkContext
  }
}
