package com.service

//import com.KafkaUtil
//import com.util.Constants
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object FlinkTest {
  def main(args: Array[String]): Unit = {
    //val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.enableCheckpointing(5000)
//    val kafkaConsumer = KafkaUtil.flinkConsumer(Constants.servers, Constants.topic)
//    val stream = env.addSource(kafkaConsumer)
//    stream.setParallelism(4).print()

//    val windowcounts = text.flatMap(line => line.split("\\s"))
//      .map(w => wordwithcount(w, 1))
//      .keyBy("word")
//      .timeWindow(Time.seconds(2), Time.seconds(1))
//      .sum("count")
//    //打印到控制台
//    windowcounts.print().setParallelism(1)

   // env.execute("FlinkKafkaStreaming")
  }


}
