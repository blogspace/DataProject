package com.repository.impl

import com.repository.IDouBanRepository
import com.uils.{AppConf, Constants, DriverUtil, SparkUtil}
import org.apache.spark.rdd.RDD

/**
  * @function 数据输入层
  */
object DouBanRepositoryImpl extends IDouBanRepository {
  val sc = DriverUtil.sc

  override def srcHotMovies: RDD[Array[String]] = {
    SparkUtil.fileSplit(sc, Constants.HotMovies)
  }

  override def srcUserMovies: RDD[Array[String]] = {
    SparkUtil.fileSplit(sc, Constants.UserMovies)
  }

}
