package com.demo.dao

import org.apache.spark.rdd.RDD

trait IChurnAnalysisDao {
  def srcTrainingData: RDD[String]

  def srcTestData: RDD[String]
}
