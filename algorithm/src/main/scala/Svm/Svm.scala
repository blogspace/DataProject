package Svm

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.log4j.{Level, Logger}
object Svm {
  def main(args: Array[String]): Unit = {
    
    //创建Spark对象
    val conf=new SparkConf().setAppName("svm")
    val sc=new SparkContext(conf)
    
    //读取样本数据，格式为 libsvm
    val data = MLUtils.loadLibSVMFile(sc,"/home/hadoop/test/svm/sample_libsvm_data.txt")
    
    //数据划分为训练样本和测试样本
    val splits= data.randomSplit(Array(0.6,0.4), seed =11L)
    val training =splits(0).cache()
    val test=splits(1)
    
    //创建逻辑回归模型并训练
    val numIterations=100//迭代次数
    val model=SVMWithSGD.train(training,numIterations)
    
    //对测试样本进行测试
    val predictionAndLabel=test.map(point=> {
      val score=model.predict(point.features)
        (score,point.label)
      })
    val print_predict=predictionAndLabel.take(20)  
    // 打印输出结果
    println("prediction"+"\t"+"label")
    for (i<- 0 to print_predict.length - 1)
    {
      println(print_predict(i)._1+"\t"+print_predict(i)._2)
    }
    
    //误差计算
    val accuracy =1.0 *predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("Area under Roc = "+accuracy)
    
    //保存模型
    val ModelPath="/home/hadoop/test/svm/svm_model"
    model.save(sc,ModelPath)
  }
 
}

