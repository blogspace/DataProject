package com.uils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * @function pro_dwd_reg_info驱动工具类
  * @create by liuhao at 2019/5/14
  */
object DriverUtil {
  private var sparkContext:SparkContext = null
  def sc={
    sparkContext=SparkUtil.createSparkContext("pro_dwd_reg_info")
    sparkContext
  }
}
