package com.demo.repository

import org.apache.spark.rdd.RDD

trait IPurchaseForecastRepositor {
  def srcAction:RDD[Array[String]]
  def srcComment:RDD[Array[String]]
  def srcProduct:RDD[Array[String]]
  def srcShop:RDD[Array[String]]
  def srcUser:RDD[Array[String]]
}
