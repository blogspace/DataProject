package com.demo.dao.impl

import com.demo.dao.IChurnAnalysisDao
import com.demo.util.{Constant, DriverUtil, SparkUtil}
import org.apache.spark.rdd.RDD

object ChurnAnalysisDaoImpl extends IChurnAnalysisDao {
  private val sc = DriverUtil.sparkContext(Constant.appName, Constant.master)

  override def srcTrainingData: RDD[String] = ???

  override def srcTestData: RDD[String] = ???
}
