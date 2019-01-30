package demo1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 文本情感分析系统
  *
  * @author aloha
  */
object SentimentAnalysis {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.jetty.server").setLevel(Level.OFF)
    /*
      82019
      物流快递
      百世汇通很差，六天没送到，打电话两天前己到深圳，几分钟的路程今天又送了一天，明天不知能不能到。这就是百世汇通!明天不到货我准备要他们退回去算了。球服务!
      0
     */
    val conf = new SparkConf().setAppName("SentimentAnalysis").setMaster("local")
    val sc = new SparkContext(conf)
    //1.加载数据
    val dataInput = sc.textFile("D:\\datatest\\user_emotion\\data_train.csv").map(
      line =>{
      val name = line.split("\t")(1)
      //println(name)
      val index = line.split("\t")(3)
      //println(index)
        name + "\t"+ index
        //println("验证："+name + "\t"+ index)
    }).filter(_.matches(\/d+\))

    println("总条数："+dataInput.count())
    dataInput.take(1).foreach(println)
    for (n <- 0 to 2) {
      val da = dataInput.filter(_.split("\t")(1).toInt == 2)
      println(n + "分的条数为：" + da.count())
    }


    //2.统计数据基本信息
    //val dataIndex = dataInput.map(x => (x,1)).reduceByKey(_ + _).foreach(println)
    //3.生成训练数据集

    //4.分词
    //    val data = sc.textFile("D:\\datatest\\user_emotion\\data_train.csv").map(x => {
    //      val list = tokens.anaylyzerWords(x)
    //      list.toString.split("\t")
    //    }).flatMap(x => x.toList).map(x => (x.trim(), 1)).reduceByKey(_ + _).top(20)(Ord.reverse).foreach(println)
    //  }
    //
    //  //分词排序
    //  object Ord extends Ordering[(String, Int)] {
    //    def compare(a: (String, Int), b: (String, Int)) = a._2 compare (b._2)
    //  }
    //5.训练词频矩阵

    //6.计算TF-IDF

    //7.生成训练集和测试集

    //8.模型训练
    sc.stop()
  }

}



