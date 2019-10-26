package com.demo

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class KeyedStateAgvFlatMap extends RichFlatMapFunction[(String,Long),(String,Long)]{
  @transient
  private var valueState: ValueState[Long] = _
  override def open(parameters: Configuration): Unit ={
    super.open(parameters)
    //验证statestate的生命周期，咱们设置了20秒，20秒state自
    动清空
    //如果你的key一直有，这个ttl会一直保持20s不清空
    //如果你的key，20s都没有见过，这个key会被清空
    val config = StateTtlConfig.newBuilder(Time.seconds(20))
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnEx
        pired)
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWri
        te)
      .build()
    val valueStateDescriptor = new ValueStateDescriptor("agvKeyedState",TypeInformation.of(new TypeHint[Long] {}))
    valueStateDescriptor.enableTimeToLive(config)
    valueState=getRuntimeContext.getState(valueStateDescripto
      r)
  }
  override def flatMap(value: (String, Long), collector: Collector[(String, Long)]): Unit = {
    var currentValue = valueState.value()
    if(currentValue.equals(null)){
      currentValue=0L
    }
    currentValue = currentValue+ value._2
    valueState.update(currentValue)
    collector.collect((value._1,currentValue))
  }
}
