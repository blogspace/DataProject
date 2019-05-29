package com.repository

import org.apache.spark.rdd.RDD

trait IDouBanRepository {
  def srcHotMovies:RDD[Array[String]]
  def srcUserMovies:RDD[Array[String]]
}
