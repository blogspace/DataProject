package demo1.SparkUtil

import org.apache.spark.sql.SparkSession

/**
  * 使用多线程从mysql加载数据到kafka中
  * @param ThreadName
  */
class SparkThread(ThreadName:String) extends Runnable{
  override def run(): Unit = {
    readMysql()
  }
  //使用sparksql读取数据
  def readMysql(): Unit = {
    //创建sparkSession
    val sparkSession = SparkSession.builder().appName("sparkOnMysql").master("local").getOrCreate()
    //表名存储
    val tableName = Array[String]("table1", "table2", "table3")
    for (i <- 0 to tableName.length) {
      //加载MySQL数据库中的数据
      val dataInput = sparkSession.sqlContext.read.format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", tableName(i))
        .option("user", "root")
        .option("password", "root")
        .load()
      dataInput.show()
      //将DataFrame注册成视图
      val dataview = dataInput.registerTempTable("demo")
      //使用sql,limit,offset方式读进数据（按照限定的数量）
      val datasql = sparkSession.sql("select * from demo limit i+1,i+10")
      //将DataFrame转换成Rdd
      val result = datasql.rdd
    }

  }
}
