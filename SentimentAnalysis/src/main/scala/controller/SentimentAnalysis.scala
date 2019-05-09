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
  * 文本情感分类
  * @author aloha
  */
object SentimentAnalysis {
  case class RawDataRecord(category: String, text:String )
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    //Logger.getLogger("org.apache.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("SentimentAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sc.setLogLevel("ERROR")

    //1.加载数据
    import sqlContext.implicits._
    val srcRDD = sc.textFile("D:\\datatest\\user_emotion\\data_train.csv").filter(_.split("\t").size==4)
      .map(line=>{
          val col = line.split("\t",-1)
       RawDataRecord(col(3),tokens.anaylyzerWords(col(2)).toArray().mkString(" "))
        })
    //dataInput.select("category", "text").take(2).foreach(println)
    val splits = srcRDD.randomSplit(Array(0.7, 0.3))
    var trainingDF = splits(0).toDF()
    var testDF = splits(1).toDF()

    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(trainingDF)

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
    }.rdd

    val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")

    //测试数据集，做同样的特征表示及格式转换
    var testwordsData = tokenizer.transform(testDF)
    var testfeaturizedData = hashingTF.transform(testwordsData)
    var testrescaledData = idfModel.transform(testfeaturizedData)

    var testDataRdd = testrescaledData.select($"category",$"features").map {
      case Row(label: String, features: mlV) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    //对测试数据集使用训练模型进行分类预测
    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))
    //统计分类准确率
    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
    println("output5：")
    println(testaccuracy)

    sc.stop()
  }
}



