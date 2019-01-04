package wordcount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MyWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val args = Array("D:\\datatest\\wordcount.txt","D:\\dataresult\\a")
    //文件配置
    val conf = new SparkConf().setAppName("MyWordCount").setMaster("local")
    //核心创建一个SparkContext
    val sc = new SparkContext(conf)
    //使用sc对象执行任务
        sc.textFile(args(0))
            .flatMap(_.split(" "))
             .map((_,1))
              .reduceByKey(_+_)
                .saveAsTextFile(args(1))
//    sc.textFile("D:\\datatest\\wordcount.txt",1)
//      .flatMap(_.split(" "))
//      .map((_,1))
//      .reduceByKey(_+_)
//      .saveAsTextFile("D:\\dataresult\\b")

    //停止SparkContext对象
    sc.stop()
  }
}
