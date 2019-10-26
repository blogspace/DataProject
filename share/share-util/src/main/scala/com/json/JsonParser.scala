package com.json

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object JsonParser {
  /**
    * @function 根据所需字段获取数据
    * @param json
    * @param schema
    * @return
    */
  def jsonParser(json:String,schema:Array[String])={
    val jsonArray = JSON.parseArray(json).toArray()
    jsonArray.map(x=>{
      val json = JSON.parseObject(x.toString)
      val array = ArrayBuffer[AnyRef]()
      for (arr <- schema) {
        val value = json.getOrDefault(arr, "")
        array += value
      }
      array.mkString("\t")
    })
  }

}
