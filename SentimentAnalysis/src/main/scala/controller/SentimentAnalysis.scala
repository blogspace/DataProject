package demo1

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.tokens

/**
  * 文本情感分析
  * @author aloha
  */
object SentimentAnalysis {
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    //Logger.getLogger("org.apache.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("SentimentAnalysis").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sc.setLogLevel("ERROR")
    //1.加载数据
    val dataInput = sc.textFile("D:\\datatest\\user_emotion\\data_train.csv")
      .filter(_.split("\t").size==4).map(line=>{
          val col = line.split("\t",-1)
      (col(3),tokens.anaylyzerWords(col(2)))
        })
//    //2.计算词频矩阵
    val hashingTF = new HashingTF(numFeatures = 2000)
    dataInput.map(line =>{
      val tf_feature = hashingTF.transform(line._2.toArray())
      val idf = new IDF()
      val idf_model = idf.fit(tf_feature) //生成IDF Model
      val tf_idf = idf_model.transform(tf_feature)

      val Array(trainData, testData) = tf_idf.randomSplit(Array(0.9, 0.1), seed = 1234L)
      val naiveBayesModel = NaiveBayes.train(trainData,lambda = 1.0)

    })




//    val hashingTF = new HashingTF(numFeatures = 2000)
//    val tf_feature = dataInput.map(line => hashingTF.transform(line._2.toArray()))
//    val idf = new IDF()
//    val idf_model = idf.fit(tf_feature) //生成IDF Model
//    val tf_idf = idf_model.transform(tf_feature)
//
//    val Array(trainData, testData) = tf_idf.randomSplit(Array(0.9, 0.1), seed = 1234L)
//    val naiveBayesModel = NaiveBayes.train(trainData,lambda = 1.0)

    //val pre=testData.map(p=>(naiveBayesModel.predict(p.features),p.label))//验证模型
//    val pre=test.map(p=>(naiveBayesModel.predict(p.features),p.label))//验证模型
//    val prin=pre.take(20)
    //Select(prediction,true label) and compute test error
//    val evaluator = new MulticlassClassificationEvaluator()
//      .setLabelCol("label")
//      .setPredictionCol("prediction")
//      .setMetricName("accuracy")
//
//    val accuracy = evaluator.evaluate(predictions)
//    println(s"Test set accuracy = $accuracy")
//
//    logger.warn(TimeUtil.getLongCurrentTime() + " end!")

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


    sc.stop()
  }

}



