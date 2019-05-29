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
    val testingData = realRatings.map{ case Rating(user, product, rate) =>
      (user, product)
    }

    val prediction = model.predict(testingData).map{ case Rating(user, product, rate) =>
      ((user, product), rate)
    }

    val realPredict = realRatings.map{case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(prediction)

    sqrt(realPredict.map{ case ((user, product), (rate1, rate2)) =>
      val err = rate1 - rate2
      err * err
    }.mean())//mean = sum(list) / len(list)
  }

  /**
    * @function 余玄相似度计算
    * @param vector1
    * @param vector2
    * @return
    */
  def cosineSimilarity(vector1: DoubleMatrix, vector2: DoubleMatrix): Double = {
    vector1.dot(vector2) / (vector1.norm2() * vector2.norm2())
  }


}
