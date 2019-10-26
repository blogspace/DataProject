package com.demo

import java.util.Properties

import kafka.producer.KeyedMessage
import org.apache.kafka.clients.producer.{Producer, ProducerConfig}

import scala.io.Source

object kafkaProduct {
  def test1() = {
    val brokers_list = "localhost:9092"
    val topic = "flink2"
    val props = new Properties()
    props.put("group.id", "test-flink")
    props.put("metadata.broker.list", brokers_list)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("num.partitions", "4")
    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    var num = 0
    for (line <- Source.fromFile("/Users/huzechen/Downloads/flinktest/src/main / resources / cep1").getLines) {
      val aa = scala.util.Random.nextInt(3).toString
      println(aa)
      producer.send(new KeyedMessage(topic, aa, line))
    }
    producer.close()
  }

  def main(args: Array[String]): Unit = {
    test1()
  }
}