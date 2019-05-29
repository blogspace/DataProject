package com.service.impl

import com.repository.impl.DouBanRepositoryImpl
import com.service.IDouBanService
import com.uils.{AppConf, ProjectUtil}
import com.uils.ProjectUtil.rating
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * @function 业务处理层
  */
object DouBanServiceImpl extends IDouBanService {
  /**
    * @function movieId和movieName进行映射
    * @return
    */
  override def movieList = {
    DouBanRepositoryImpl.srcHotMovies.map(line => (line(0), line(2).toString))
      .persist(StorageLevel.MEMORY_AND_DISK).collectAsMap()
  }

  /**
    * @function 对UserMovies打分表进行预处理
    * @return
    */
  override def userRating = {
    DouBanRepositoryImpl.srcUserMovies.map(line => rating(line(0), line(1).toInt, ProjectUtil.MissingValue(line(2))))
      .persist(StorageLevel.MEMORY_AND_DISK)
  }
  /**
    * @function 根据用户id推荐电影
    * @return
    */
  override def recommendation = {
    val movie = DouBanRepositoryImpl.sc.broadcast(movieList)
    val userId = userRating.map(_.userID).zipWithIndex().map{case(x,y)=>(x.toString,y.toInt)}.collectAsMap()
    val Iduser = userId.map{case (x,y)=>(y,x)}
    val ratings = userRating.map(line => Rating(userId.apply(line.userID), line.movieID, line.rating))
    val model = ALS.train(ratings, 50, 10, 0.0001)

    model.recommendProductsForUsers(5).map(line => {
      var arr = new ArrayBuffer[Any]()
      arr += Iduser.get(line._1).get
      line._2.map(x => {
        arr+= movie.value.get(x.product.toString).get
      })
     arr(0)+":"+arr(1)+","+arr(2)+","+arr(3)+","+arr(4)+","+arr(5)
    })
  }

  def main(args: Array[String]): Unit = {
    recommendation.foreach(println)
  }
}
