package com.demo.service

import org.apache.spark.rdd.RDD

trait IChurnAnalysisService {

  def dataClean:RDD[String]

  def dataExplore:RDD[String]

  def dataModel:RDD[String]

  def modelForecast:RDD[String]



}
