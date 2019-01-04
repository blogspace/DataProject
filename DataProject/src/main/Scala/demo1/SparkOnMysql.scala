package demo1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkOnMysql {
  def main(args: Array[String]): Unit = {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 //创建sparkSession
  val sparkSession = SparkSession.builder().appName("sparkOnMysql").master("local").getOrCreate()
  //加载MySQL数据库
  val data = sparkSession.sqlContext.read.format("jdbc")
    .option("url","jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8")
    .option("driver","com.mysql.jdbc.Driver")
    .option("dbtable","demotest")
    .option("user","root")
    .option("password","root")
    .load()
  data.show()











  }

}
