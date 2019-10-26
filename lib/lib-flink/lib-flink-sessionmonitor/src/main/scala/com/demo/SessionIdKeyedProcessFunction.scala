package com.demo

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.functions._
import org.apache.flink.streaming.api.watermark.Watermark

object SessionIdKeyedProcessFunction {

  class MyTimeTimestampsAndWatermarks extends AssignerWithPunctuatedWatermarks[(String, String)] with Serializable {
    //生成时间戳
    override def extractTimestamp(element: (String, String), previousElementTimestamp: Long): Long = {
      System.currentTimeMillis()
    }

    //获取wrtermark
    override def checkAndGetNextWatermark(lastElement: (String, String), extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp - 1000)
    }
  }

  case class SessionInfo(session_id: String, event_id: String, timestamp: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val properties = new Properties()
    //kafka位置 老版本的 kafka是配置zookeeper地址
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181"
    )
    val topic = "flink2"
    properties.setProperty("group.id", "test-flink")
    //初始化读取kafka的实时流
    val consumer = new FlinkKafkaConsumer08(topic, new SimpleStringSchema(), properties)
    val text: DataStream[Tuple2[String, String]] = env.addSource(consumer).map(line => {
      val json = JSON.parseObject(line)
      //返回用户session_id,用户事件event_id
      Tuple2(json.get("session_id").toString, json.get("event_id").toString)
    }).assignTimestampsAndWatermarks(new MyTimeTimestampsAndWatermarks())
    text.keyBy(0).process(new SessionIdTimeoutFunction()).setParallelism(1).print()
    env.execute()
    //由于是按key聚合，创建每个key的状态 key=session_id
    //实现KeyedProcessFunction内的onTime方法
    class SessionIdTimeoutFunction extends KeyedProcessFunction[Tuple, (String, String), (String, String)] {
      private var state: ValueState[SessionInfo] = _
      private var sdf = new SimpleDateFormat("yyyy-MM-dd HH: mm: ss")

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val config = StateTtlConfig.newBuilder(Time.minutes(5))
          .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
          .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
          .build()
        val valueStateDescriptor = new ValueStateDescriptor("myState1", classOf[SessionInfo])
        valueStateDescriptor.enableTimeToLive(config)
        state = getRuntimeContext.getState(valueStateDescriptor)
      }

      override def processElement(message: (String, String), ctx: KeyedProcessFunction[Tuple, (String, String), (String, String)]#Context, out: Collector[(String, String)]) = {
        //用户sessionid用户行为轨迹
        if (state.value() == null) {
          val timeStamp = ctx.timestamp()
          //输出当前实时流事件，这次没有考虑事件先后顺序
          //如果要对事件先后顺序加一下限制，state需要重新设计
          //这次就简单实现一下原理，后边我再写一个针对顺序的代码
          out.collect((message))
          //如果状体是A,设置下次回调的时间。5秒之后回调
          if (message._2 == "A") {
            ctx.timerService.registerEventTimeTimer(timeStamp + 5000)
            state.update(SessionInfo(message._1, message._2, timeStamp)
            )
          }
        }
        //如果发现当前sessionid下有B行为，就更新B
        println("当前时间：" + sdf.format(new Date(ctx.timestamp)))
        if (message._2 == "B") {
          state.update(SessionInfo(message._1, message._2, ctx.timestamp()))
        }
      }

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, (String, String), (String, String)]#OnTimerContext, out: Collector[(String, String)]): Unit = {
        //如果当前key，5秒之后，没有触发B事件
        //并且事件一定到了触发的事件点，就输出C事件
        println("onTimer触发时间：状态记录的时间_触发时间" + sdf.format(new Date(state.value().timestamp)) + " _ " + sdf.format(new Date(timestamp)))
        if (state.value().event_id != "B" && state.value().timestamp + 5000 == timestamp) {
          out.collect(("SessionID为：" + state.value().session_id, "由于5s内没有看到B触发C时间"))
        }
      }
    }
  }
}