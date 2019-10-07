package com.demo.dao.impl

import com.demo.dao.IPurchaseForecastDao
import com.demo.util.{Constant, DriverUtil, SparkUtil}
import org.apache.spark.rdd.RDD

object PurchaseForecastDaoImpl extends IPurchaseForecastDao {
  private val sc = DriverUtil.sparkContext(Constant.appName, Constant.master)

  override def srcAction: RDD[Array[String]] = {
    SparkUtil.fileSplit(sc, Constant.action)
  }

  override def srcComment: RDD[Array[String]] = {
    SparkUtil.fileSplit(sc, Constant.comment)
  }

  override def srcProduct: RDD[Array[String]] = {
    SparkUtil.fileSplit(sc, Constant.product)
  }

  override def srcShop: RDD[Array[String]] = {
    SparkUtil.fileSplit(sc, Constant.shop)
  }

  override def srcUser: RDD[Array[String]] = {
    SparkUtil.fileSplit(sc, Constant.user)
  }

  def main(args: Array[String]): Unit = {
    srcUser.foreach(x => println(x.mkString("\t")))
  }

}
