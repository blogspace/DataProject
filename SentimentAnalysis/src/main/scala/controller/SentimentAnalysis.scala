package demo1

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.{Vector => mlV}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.tokens

/**
  * 文本情感分析
  * @author aloha
  */
object SentimentAnalysis {
  case class RawDataRecord(category: String, text:String )
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    //Logger.getLogger("org.apache.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("SentimentAnalysis").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sc.setLogLevel("ERROR")

    //1.加载数据
    import sqlContext.implicits._
    val dataDF = sc.textFile("D:\\datatest\\user_emotion\\data_train.csv").filter(_.split("\t").size==4)
      .map(line=>{
          val col = line.split("\t",-1)
       RawDataRecord(col(3),tokens.anaylyzerWords(col(2)).toArray().mkString(" "))
        }).toDF()
    //dataInput.select("category", "text").take(2).foreach(println)

    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(dataDF)

    wordsData.select($"category",$"text",$"words").take(2)

    var hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(500000)
    var featurizedData = hashingTF.transform(wordsData)
    featurizedData.select($"category", $"words", $"rawFeatures").take(2)

    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)
    rescaledData.select($"category", $"words", $"features").take(2)

    var trainDataRdd = rescaledData.select($"category",$"features").map {
      case Row(label: String, features: mlV) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    println("output4：")
    trainDataRdd.show()
//
   val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")
//
    //测试数据集，做同样的特征表示及格式转换
//    var testwordsData = tokenizer.transform(testDF)
//    var testfeaturizedData = hashingTF.transform(testwordsData)
//    var testrescaledData = idfModel.transform(testfeaturizedData)
//    var testDataRdd = testrescaledData.select($"category",$"features").map {
//      case Row(label: String, features: mlV) =>
//        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
//    }

    //对测试数据集使用训练模型进行分类预测
//    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))
//
//    //统计分类准确率
//    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
//    println("output5：")
//    println(testaccuracy)
    //2.计算词频矩阵
//     val hashingTF = new HashingTF(numFeatures = 2000)
//    val tf_feature = hashingTF.transform(dataInput)
//    val tf_feature = dataInput.map(line => {
//      (line._1,hashingTF.transform(line._2))
//    }).foreach(println)
//    val idf = new IDF()
//    val idf_model = idf.fit(tf_feature)
//    val tf_idf = tf_feature.map(line =>(line._1,idf_model.transform(line._2))).foreach(println)
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
//    val accuracy = evaluator.evaluate(predictions)
//    println(s"Test set accuracy = $accuracy")
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



