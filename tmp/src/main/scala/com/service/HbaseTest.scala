package com.service

import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HbaseTest {
   var puts = List[Put]()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("logdetail").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val pairRdd = sc.textFile("D:\\admin\\Desktop\\log").map(x => {
      val col = x.split("\t", -1)
      Array(col(0), col(1), col(2), col(3), col(4), col(5), col(6), col(7), col(8), col(9), col(10), col(11), col(12), col(13))
    })
   writeToHBase(pairRdd,"log:logClean")
    def writeToHBase(rdd: RDD[Array[String]], tableName: String) = {
      //val tableName = "log:logClean"
      //创建HBase配置
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.192.10") //设置zookeeper集群，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
      hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181") //设置zookeeper连接端口，默认2181
      hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

      //初始化job，设置输出格式，TableOutputFormat 是 org.apache.hadoop.hbase.mapred 包下的
      val jobConf = new JobConf(hbaseConf)
      jobConf.setOutputFormat(classOf[TableOutputFormat])

      val data = rdd.map(line => {
        val key: String = (line(0) + "_" + line(1) + "_" + line(2) + "_" + line(4)).replace("-", "").replace(" ", "").replace(":", "").trim
        //val rowKey = key.reverse
        val put = new Put(Bytes.toBytes(key))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("timestr"), Bytes.toBytes(line(0)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("offsetNum"), Bytes.toBytes(line(1)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("app_user_id"), Bytes.toBytes(line(2)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("vid"), Bytes.toBytes(line(3)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("v_time"), Bytes.toBytes(line(4)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("formtype"), Bytes.toBytes(line(5)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("url"), Bytes.toBytes(line(6)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("referrer_url"), Bytes.toBytes(line(7)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("event"), Bytes.toBytes(line(8)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("type"), Bytes.toBytes(line(9)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("app_id"), Bytes.toBytes(line(10)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("channel_id"), Bytes.toBytes(line(11)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("device_id"), Bytes.toBytes(line(12)))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("extra_id"), Bytes.toBytes(line(13)))
        (new ImmutableBytesWritable(), put)
      })
      //保存到HBase表
      data.saveAsHadoopDataset(jobConf)
    }




//    val column = Array("timestr", "offsetNum", "app_user_id", "vid", "v_time", "formtype", "url", "referrer_url", "event", "type", "app_id", "channel_id", "device_id", "extra_id")
//    //loadHbase(sc, "log:logClean").take(100).foreach(println)
//    try {
//      val table: String = "log:logClean"
//
//      pairRdd.foreach(x => {
//        val key: String = (x(0) + "_" + x(1) + "_" + x(2) + "_" + x(4)).replace("-", "").replace(" ", "").replace(":", "").trim
//        val put: Put = new Put(Bytes.toBytes(key))
//
//        for (i <- column.indices) {
//          put.addColumn("info".getBytes, column(i).getBytes, x(i).getBytes)
//        }
//        puts = puts ::: List(put)
//
//
//      })
//
//     HBaseUtil.addRows(table, scala.collection.JavaConversions.seqAsJavaList(puts))
//      //conn.close()
//    }
//    catch {
//      case e: ArithmeticException => println(e)
//    }

    sc.stop()
  }
}
