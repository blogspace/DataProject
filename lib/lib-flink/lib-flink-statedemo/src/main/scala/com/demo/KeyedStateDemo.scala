package com.demo

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.kafka.clients.producer.{Producer, ProducerConfig}

import scala.io.Source

object KeyedStateDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    //kafka位置 老版本的 kafka是配置zookeeper地址
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("zookeeper.connect","localhost:2181"
    )
    val topic = "click"
    properties.setProperty("group.id", "test-flink")
    val consumer = new FlinkKafkaConsumer08(topic,new SimpleStringSchema(),properties)
    val input = env.addSource(consumer).map(line => (line,1L))
    input.keyBy(0)
      .flatMap(new KeyedStateAgvFlatMap())
      .setParallelism(10)
      .print()
    env.execute()
  }


}
