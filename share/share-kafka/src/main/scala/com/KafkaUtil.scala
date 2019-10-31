package com

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * @function kafka操作工具类
  */
object KafkaUtil {
  /**
    * @function spark write data to kafka
    * @param lines
    * @param kafkaTopic
    * @param servers
    */
  def produces(lines: RDD[String], servers: String, kafkaTopic: String) = {
    val kafkaParams = new Properties()
    // 指定kafka的 ip地址:端口号
    kafkaParams.put("bootstrap.servers", servers) //servers:9092
    // 设定ProducerRecord发送的key值为String类型
    kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 设定ProducerRecord发送的value值为String类型
    kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](kafkaParams)
    lines.toLocalIterator.foreach(line => {
      val record = new ProducerRecord[String, String](kafkaTopic, line)
      producer.send(record)
      //println(line)
      Thread.sleep(10)
    })
    producer.close()
  }

  /**
    * @function spark read data on kafka
    * @param ssc
    * @param servers
    * @param kafkaTopic
    */
  def consumes(ssc: StreamingContext, servers: String, kafkaTopic: String) = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> servers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "0001",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topic = Array(kafkaTopic)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](topic, kafkaParams)
    ).map(line => line.value())
    stream
  }

  /**
    * @function 将offset保存至mysql
    * @param ssc
    * @param servers
    * @param kafkaTopic
    * @return
    */
  // val rddOffset = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
  // OffsetManager.saveCurrentBatchOffset("", rddOffset)
  def consumes(ssc: StreamingContext, servers: String, kafkaTopic: String) = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> servers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "0001",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topic = Array(kafkaTopic)
    val offsetManager = OffsetManager("", kafkaTopic)
    val stream = if (offsetManager.isEmpty) {
      KafkaUtils.createDirectStream[String, String](
        ssc, PreferConsistent, Subscribe[String, String](topic, kafkaParams)
      )
    } else {
      KafkaUtils.createDirectStream[String, String](
        ssc, PreferConsistent, Subscribe[String, String](topic, kafkaParams, offsetManager)
      )
    }
    stream
  }

  //    def consumeData(ssc:StreamingContext,servers:String,kafkaTopic:String) ={
  //      val topics = Set(kafkaTopic)
  //      val kafkaParams = Map("metadata.broker.list" -> servers)
  //      val messages = Kafka.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
  //      messages
  //    }

  /**
    * @function flink write data to kafka
    * @param stream
    * @param servers
    * @param topic
    * @return
    */
  def produce(stream: DataStream[String], servers: String, topic: String) = {
    val myProducer = new FlinkKafkaProducer[String](servers, topic, new SimpleStringSchema)
    myProducer.setWriteTimestampToKafka(true)
    stream.addSink(myProducer)
  }

  /**
    * @function flink read data on kafka
    * @param servers
    * @param groupId
    * @param topic
    * @return
    */
  def consume(servers: String, topic: String) = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", servers)
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("group.id", "test")
    properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("enable.auto.commit", "false")
    new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
  }

}
