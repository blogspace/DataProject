package com.demo.util

object Constant {
  //sparkConf参数
  val appName = "ChurnAnalysis"
  val master = "local[*]"
  //数据源
  val src = "D:\\workspace\\idea\\DataProject\\ChurnAnalysis\\data\\"
  val testData = s"${src}churn-bigml-20.csv"
  val trainingData = s"${src}churn-bigml-80.csv"

}
