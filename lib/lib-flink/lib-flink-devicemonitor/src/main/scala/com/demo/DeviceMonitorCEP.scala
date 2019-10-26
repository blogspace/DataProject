package com.demo

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation, Types}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object DeviceMonitorCEP {
  def main(args: Array[String]): Unit = {
    implicit val typeInfo = TypeInformation.of(classOf[(RegionInfo)])
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val descriptor: MapStateDescriptor[String, RegionInfo] = new MapStateDescriptor("region_rule", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint[RegionInfo] {}))
    val regionStream = env.addSource(new JdbcReader).broadcast(descriptor)
    val properties = new Properties()
    //kafka位置 老版本的 kafka是配置zookeeper地址
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181"
    )
    val topic = "flink-topic"
    properties.setProperty("group.id", "test-flink")
    val textConsumer = new FlinkKafkaConsumer08(topic, new SimpleStringSchema(), properties)
    env.addSource(textConsumer).connect(regionStream).process(new BroadcastProcessFunction[String, RegionInfo, String] {
      override def processBroadcastElement(regionInfo: RegionInfo, ctx: BroadcastProcessFunction[String, RegionInfo, String]#Context, out: Collector[String]) = {
        val region = regionInfo.region
        val state = ctx.getBroadcastState(descriptor)
        state.put(region + "_" + regionInfo.inner_code, regionInfo)
      }

      override def processElement(value: String, ctx:
      BroadcastProcessFunction[String, RegionInfo, String]#ReadOnlyContext, out: Collector[String]) = {
        val json = JSON.parseObject(value)
        val arrayJson = json.getJSONArray("risks")
        for (i <- 0 until arrayJson.size()) {
          val obj = arrayJson.getJSONObject(i)
          val state = ctx.getBroadcastState(descriptor).immutableEntries().iterator()
          while (state.hasNext) {
            val map = state.next()
            if (obj.get(map.getKey.split("_")(0)) ==
              map.getValue.value) {
              obj.put("inner_code", map.getValue.inner_code)
              out.collect(obj.toJSONString)
            }
          }
        }
      }
    }).setParallelism(1).print()
    env.execute()
  }
}