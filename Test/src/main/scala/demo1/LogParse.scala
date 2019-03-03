package demo1

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import net.minidev.json.{JSONArray, JSONObject}
import net.minidev.json.parser.JSONParser
import scala.collection.mutable
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConversions.mutableMapAsJavaMap

object LogParse {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("logparse").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val line = sc.textFile("D:\\data\\hf-lib-other-logconsume.log").filter(x => x.contains("[ERROR]"))
    val data = line.map(x => {
      x.substring(x.indexOf("{"), x.lastIndexOf("}") + 1)
    })
    val time = line.map(x => x.split(" \\[ERROR] ")(0))
    //time.foreach(println)
   // val map : mutable.HashMap[String,Object]= mutable.HashMap()
    val json= data.map(x => jsonToMap(x))
    json.map(x => x.get("msgs")).foreach(println)
    //    val key = json.toString().asInstanceOf[JSONObject]
//    key.keySet()
    //获取所有键
//    val iter = jsonKey.iterator()
//    while(iter.hasNext){
//      val field = iter.next()
//      val value = jsonObj.get(field).toString
//      val map : mutable.HashMap[String,Object]= mutable.HashMap()
//      val v = value.iterator
//      while(v.hasNext){
//        val field = iter.next()
//        val value = jsonObj.get(field).toString
//        map.put(field,value)
//      }
//      map.foreach(println)
//    }
//      val result = data.map(s => JSON.parseFull(s));//逐个JSON字符串解析
//      result.foreach(
//      {
//        r => r match {
//          case Some(map:Map[String,Any]) => (printlnmap)
//          case None => println("parsing failed!")
//          case other => println("unknown data structure" + other)
//        }
//      }
//    )
  }

  def jsonToMap(json : String) : mutable.HashMap[String,Object] = {
    val map : mutable.HashMap[String,Object]= mutable.HashMap()
    val jsonParser =new JSONParser()
    //将string转化为jsonObject
    val jsonObj: JSONObject = jsonParser.parse(json).asInstanceOf[JSONObject]
    //    //获取所有键
    val jsonKey = jsonObj.keySet()
    val iter = jsonKey.iterator()
    while (iter.hasNext){
      val field = iter.next()
      val value = jsonObj.get(field).toString
      if(value.startsWith("[")&&value.endsWith("]")){
        val value1 = jsonObj.get(field).asInstanceOf[JSONArray]
        for(i <- 0 to value1.size()-1){
          val value2 = jsonToMap(value1.get(i).toString)
                 map.put(field,value2)
        }
      }else if(value.startsWith("{")&&value.endsWith("}")){
        val value3 = jsonToMap(value)
        map.put(field,value3)
      }else{
        map.put(field,value)
      }
    }
    map
  }
 }
