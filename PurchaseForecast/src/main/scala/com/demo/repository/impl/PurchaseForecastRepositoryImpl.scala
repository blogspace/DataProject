package com.demo.repository.impl

import com.demo.repository.IPurchaseForecastRepositor
import com.demo.utils.{AppConf, Constants, SparkUtil}
import org.apache.spark.rdd.RDD

object PurchaseForecastRepositoryImpl extends IPurchaseForecastRepositor with AppConf {
  override def srcAction: RDD[Array[String]] = {
    SparkUtil.fileSplit(sc,Constants.action)
  }

  override def srcComment: RDD[Array[String]] = {
    SparkUtil.fileSplit(sc,Constants.comment)
  }

  override def srcProduct: RDD[Array[String]] = {
    SparkUtil.fileSplit(sc,Constants.product)
  }

  override def srcShop: RDD[Array[String]] = {
    SparkUtil.fileSplit(sc,Constants.shop)
  }

  override def srcUser: RDD[Array[String]] = {
    SparkUtil.fileSplit(sc,Constants.user)
  }

  def main(args: Array[String]): Unit = {
    srcUser.foreach(x=>println(x.mkString("\t")))
  }

}
