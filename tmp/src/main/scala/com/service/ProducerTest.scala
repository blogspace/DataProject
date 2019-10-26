package com.service


import org.apache.spark.{SparkConf, SparkContext}

object ProducerTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kafkastset").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")
    val data = sc.textFile("D:\\admin\\Desktop\\log")
    //data.foreach(println)
    //KafkaUtil.produceData(data,Constants.servers,Constants.topic)
    sc.stop()
  }

}
