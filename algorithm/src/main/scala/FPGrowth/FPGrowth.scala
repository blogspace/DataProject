package FPGrowth

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }

object FPGrowth {

  def main(args:Array[String]) ={
    // 设置运行环境
    val conf = new SparkConf().setAppName("FPGrowth")
      .setMaster("spark://master:7077").setJars(Seq("E:\\Intellij\\Projects\\MachineLearning\\MachineLearning.jar"))
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 读取样本数据并解析
    val dataRDD = sc.textFile("hdfs://master:9000/ml/data/sample_fpgrowth.txt")
    val exampleRDD = dataRDD.map(_.split(" ")).cache()

    // 建立FPGrowth模型,最小支持度为0.4
    val minSupport = 0.4
    val numPartition = 10
    val model = new FPGrowth().
      setMinSupport(minSupport).
      setNumPartitions(numPartition).
      run(exampleRDD)

    // 输出结果
    println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ":" + itemset.freq)
    }
  }

}