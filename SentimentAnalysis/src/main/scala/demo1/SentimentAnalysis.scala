package demo1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 文本情感分析系统
  * @author aloha
  */
object SentimentAnalysis {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    /*
      4	食品餐饮	越来越不好了，菜品也少了，服务也不及时。	0
      5	食品餐饮	是在是不知道该吃什么好、就来了	1
      6	食品餐饮	跟这家公司合作过一次，系统功能很强大，运行很流畅，很不错的。。	2
     */
    //1.读入数据
    val conf = new SparkConf().setAppName("SentimentAnalysis").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val dataInput = sc.textFile("D:\\datatest\\user_emotion\\data_train.csv")
    val data = dataInput.map(_.split(" ").toSeq)

    //2.文本分词
    val tokenizer = new Tokenizer().setInputCol("line").setOutputCol("words")
    val wordsData = tokenizer.transform()
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select().take(3).foreach(println)

    //3.情感词频统计

    //4.模型训练

    //5.模型评估

    //6.对测试集进行测试


  }


}
