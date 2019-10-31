package com.hbase

import java.util.Base64

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job

/**
  * @Author: Yang JianQiu
  * @Date: 2019/1/25 2:27
  *        HBaseContext Usage Example
  *        参考资料：https://my.oschina.net/uchihamadara/blog/2032481
  */
class HBaseOnBasicSpark {

  /**
    * saveAsHadoopDataset
    */
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

  /**
    * saveAsNewAPIHadoopDataset
    */
  def writeToHBaseNewAPI(rdd: RDD[Array[String]], tableName: String) = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.OUTPUT_TABLE, tableName)

    val jobConf = new JobConf(hbaseConf)
    //设置job的输出格式
    val job = Job.getInstance(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[ImmutableBytesWritable]])

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
      (new ImmutableBytesWritable, put)
    })
    //保存到HBase表
    data.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def loadHbase(sc: SparkContext, tableName: String) = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.192.10")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181") //设置zookeeper连接端口，默认2181
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, tableName)

    //读取数据并转化成rdd TableInputFormat是org.apache.hadoop.hbase.mapreduce包下的
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    hbaseRDD.map { case (_, result) =>
      //获取行健
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val timestr = Bytes.toString(result.getValue("info".getBytes(), "timestr".getBytes()))
      val offsetNum = Bytes.toString(result.getValue("info".getBytes(), "offsetNum".getBytes()))
      val app_user_id = Bytes.toString(result.getValue("info".getBytes(), "app_user_id".getBytes()))
      val vid = Bytes.toString(result.getValue("info".getBytes(), "vid".getBytes()))
      val v_time = Bytes.toString(result.getValue("info".getBytes(), "v_time".getBytes()))
      val formtype = Bytes.toString(result.getValue("info".getBytes(), "formtype".getBytes()))
      val url = Bytes.toString(result.getValue("info".getBytes(), "url".getBytes()))
      val referrer_url = Bytes.toString(result.getValue("info".getBytes(), "referrer_url".getBytes()))
      val event = Bytes.toString(result.getValue("info".getBytes(), "event".getBytes()))
      val typed = Bytes.toString(result.getValue("info".getBytes(), "type".getBytes()))
      val app_id = Bytes.toString(result.getValue("info".getBytes(), "app_id".getBytes()))
      val channel_id = Bytes.toString(result.getValue("info".getBytes(), "channel_id".getBytes()))
      val device_id = Bytes.toString(result.getValue("info".getBytes(), "device_id".getBytes()))
      val extra_id = Bytes.toString(result.getValue("info".getBytes(), "extra_id".getBytes()))
      (key, timestr, offsetNum, app_user_id)
    }
  }


  /**
    * scan
    */
  def readFromHBaseWithHBaseNewAPIScan(): Unit = {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val sparkSession = SparkSession.builder().appName("SparkToHBase").master("local").getOrCreate()
    val sc = sparkSession.sparkContext

    val tableName = "test"

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.187.201")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, tableName)

    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("cf1"))
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = new String(Base64.getEncoder.encode(proto.toByteArray))
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN, scanToString)

    //读取数据并转化成rdd TableInputFormat是org.apache.hadoop.hbase.mapreduce包下的
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val dataRDD = hbaseRDD
      .map(x => x._2)
      .map { result =>
        (result.getRow, result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")), result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age")))
      }.map(row => (new String(row._1), new String(row._2), new String(row._3)))
      .collect()
      .foreach(r => (println("rowKey:" + r._1 + ", name:" + r._2 + ", age:" + r._3)))
  }
}