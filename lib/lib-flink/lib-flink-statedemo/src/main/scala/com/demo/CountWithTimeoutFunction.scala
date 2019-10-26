package com.demo

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class CountWithTimeoutFunction extends KeyedProcessFunction[Tuple, (String, String), (String,String)]{
  private var state: ValueState[DeviceInfo] = _
  val simpleDateFormat = new SimpleDateFormat("YYYYMMDD")
  override def open(parameters: Configuration): Unit ={
    super.open(parameters)
    val config = StateTtlConfig.newBuilder(Time.days(2))
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .build()
    val valueStateDescriptor =new ValueStateDescriptor("myState1", classOf[DeviceInfo])
    valueStateDescriptor.enableTimeToLive(config)
    state = getRuntimeContext.getState(valueStateDescriptor)
  }
  override def processElement(message: (String, String), ctx: KeyedProcessFunction[Tuple, (String, String), (String, String)]#Context, out: Collector[(String,String)]) = {
    // initialize or retrieve/update the state
    //如果状态为空直接返回message,说明为这个key今天第一次遇见，并更新state的value等于今天
    //如果这个key的state存储的日期，小于我今天的日期，说明我今天第一次遇见,返回这条message
    if(state.value() == null||state.value().dt <
      simpleDateFormat.format(new Date(ctx.timestamp()))){
      out.collect((message))
      state.update(DeviceInfo(message._1,simpleDateFormat.format(new Date(ctx.timestamp()))))
    }
    state.update(state.value())
  }
}
