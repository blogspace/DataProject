package com.service

import com.uils.ProjectUtil.rating
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql

import scala.collection.Map

/**
  * @function 业务处理层
  */
trait IDouBanService{
  def movieList:Map[String, String]
  def userRating:RDD[rating]
  def recommendation:RDD[String]
}
