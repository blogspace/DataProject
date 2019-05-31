package com.uils

import breeze.numerics.sqrt
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix

/**
  * @function 计算工具类
  */
object ComputeUtils {
  /**
    * @function 均方根误差计算
    * @param model
    * @param realRatings
    * @return
    */
  def computeRmse(model: MatrixFactorizationModel, realRatings: RDD[Rating]): Double = {
    val testingData = realRatings.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val prediction = model.predict(testingData).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }
    val realPredict = realRatings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(prediction)
    sqrt(realPredict.map { case ((user, product), (rate1, rate2)) =>
      val err = rate1 - rate2
      err * err
    }.mean()) //mean = sum(list) / len(list)
  }

  /**
    * @function 余玄相似度
    * @param vector1
    * @param vector2
    * @return
    */
  def cosineSimilarity(vector1: DoubleMatrix, vector2: DoubleMatrix): Double = {
    vector1.dot(vector2) / (vector1.norm2() * vector2.norm2())
  }

  /**
    * @function 计算某个物品与所有物品的相似度
    * @param model
    * @param itemId
    * @return
    */
  def allCosineSimilarity(model: MatrixFactorizationModel, itemId: Int) = {
    import org.jblas.DoubleMatrix
    //定义计算输入量为向量的余弦形式度公式
    def consineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
      vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
    }

    val itemFactor = model.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)
    consineSimilarity(itemVector, itemVector)
    //计算各个物品的相似度
    model.productFeatures.map { case (id, factor) =>
      val factorVector = new DoubleMatrix(factor)
      val sim = consineSimilarity(factorVector, itemVector)
      (id, sim)
    }
  }



}
