package NaiveBayes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{NaiveBayes,NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext,SparkConf}

object NaiveBayesTest {
   def main(args: Array[String]): Unit = {
      // 屏蔽日志
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.apache.jetty.server").setLevel(Level.OFF)
    
      //创建SparkContext
      val conf = new SparkConf().setMaster("local").setAppName("NaiveBayes")
      val sc = new SparkContext(conf)
     
      //读取样本数据
      val data = sc.textFile("D:\\datatest\\test.txt")
      
      //将数据转化为LabeledPoint格式
      val parsedData = data.map { line =>
          val parts =line.split(',')
          LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      }
     
      //样本划分，训练样本占0.6，测试样本占0.4
      val splits = parsedData.randomSplit(Array(0.6,0.4),seed = 11L)
      
      val training = splits(0)
        val test = splits(1)   
     
      //贝叶斯建模并训练模型,第一个参数为数据，第二个参数为平滑参数，默认为1，可改变
      val model =NaiveBayes.train(training,lambda= 1.0)
     
      //对测试样本进行测试      
      val predictionAndLabel= test.map(p => (model.predict(p.features),p.label))
      val print_predict=predictionAndLabel.take(20)            
      for (i<- 0 to print_predict.length - 1)
      {
        println(print_predict(i)._1  +  "\t"  +  print_predict(i)._2)
      }
      
      //贝叶斯准确度
      val accuracy =1.0 *predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()            
      println("NaiveBayes精度----->" + accuracy)
      
      //我们这里特地打印一个预测值：假如一天是   晴天(0)凉(2)高(0)高(1) 踢球与否
     // println("假如一天是   晴天(0)凉(2)高(0)高(1) 踢球与否:" + model.predict(Vectors.dense(0.0,2.0,0.0,1.0)))

      //保存model
      val ModelPath = "D:\\datatest\\model"
      model.save(sc,ModelPath)
          
    }
}