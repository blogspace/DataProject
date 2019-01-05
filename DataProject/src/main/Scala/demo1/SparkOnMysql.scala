package demo1

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkOnMysql {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.OFF)
    //创建sparkSession
    val sparkSession = SparkSession.builder().appName("sparkOnMysql").master("local").getOrCreate()
    //加载MySQL数据库中的数据
    val dataInput = sparkSession.sqlContext.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "demotest")
      .option("user", "root")
      .option("password", "root")
      .load()
    dataInput.show()
    //将DataFrame注册成视图
    val tempview = dataInput.registerTempTable("demo")
    //过滤数据
    val sqltest = sparkSession.sql("select * from demo")
    //将DataFrame转换成Rdd
    val result = sqltest.rdd
    //将数据写入kafka
    //writeToKafkaTopic(result,"192.168.99.50","da-user")

  }
  def writeToKafkaTopic(lines: RDD[String], kafkaServer: String, kafkaTopic: String): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", kafkaServer)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](properties)
    for (line <- lines) {
      val message = new ProducerRecord[String,String]("da-user","key","line")
      producer.send(message)
      producer.close()
    }
  }
}
