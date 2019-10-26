package com.demo.controller

import org.apache.spark.{SparkConf, SparkContext}

object SparkExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("")
    val sc = new SparkContext(conf)
    //设置序列化器为kyroSerializer
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    //注册要序列化的类
    conf.registerKryoClasses(Array(classOf[SparkConf], classOf[SparkContext]))
  }

}
