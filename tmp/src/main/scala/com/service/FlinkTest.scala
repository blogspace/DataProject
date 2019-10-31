package com.service

import com.util.Constants

import com.KafkaUtil
import com.util.Constants
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object FlinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    val kafkaConsumer = KafkaUtil.consume(Constants.servers, Constants.topic)
    val stream = env.addSource(kafkaConsumer)
    stream.setParallelism(4).print()
    env.execute("FlinkKafkaStreaming")
  }


}
