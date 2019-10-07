package com.jianbing.main

import com.jianbing.utils.{ Constants, KafkaCommonUtils}
import org.apache.spark.{SparkConf, SparkContext}

object ProducerTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kafkastset").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")
    val data = sc.textFile("D:\\admin\\Desktop\\data\\dwo_user_app_opt\\data\\app_accounts.txt")
    KafkaCommonUtils.produceData(data,Constants.servers,Constants.topic)
    sc.stop()
  }
}
