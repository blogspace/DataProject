package demo1

import org.apache.spark.{SparkConf, SparkContext}

object Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")
    sc.textFile("D:\\data\\JDATA训练数据\\jdata\\jdata_user.csv").take(1000).foreach(println)
  }


}
