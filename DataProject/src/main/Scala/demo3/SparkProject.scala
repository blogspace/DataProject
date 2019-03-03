package demo3

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @author liuhao
  * @description spark改写存储过程
  * @date 2019/2/22
  */
object SparkProject {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.jetty.server").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().appName("testdemo").master("local[*]").getOrCreate()
    sparkSession.conf.set("spark.sql.crossJoin.enabled", "true")
    //创建jdbc连接
    val data = sparkSession.sqlContext.read.format("jdbc")
    //表名存储
    val tableName = Array[String]("dm_shebao_query_new", "dwd_user_base_tag", "ods_jobdb_mem_shebao", "ods_shebaodb_cfg_shebao", "ods_zfgjj_cfg_app_type")
    /**
      * 1.数据加载
      */
    for (i <- 0 to tableName.length - 1) {
        if (i == 0) {
        println("加载表:" + tableName(i))
        data
          .option("url", "jdbc:mysql://localhost:3306/dm?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("dbtable", tableName(i))
          .option("user", "root")
          .option("password", "root")
          .load().createOrReplaceTempView("dm_shebao_query_new")
      } else if (i == 1) {
        println("加载表:" + tableName(i))
        val url = "jdbc:mysql://localhost:3306/dwd?useUnicode=true&characterEncoding=utf-8&?useOldAliasMetadataBehavior=true"
        val table = tableName(i)
        val columnName = "user_sid"
        val lowerBound = 1
        val upperBound = 6000000
        val numPartitions = 10
        val prop = new Properties
        prop.setProperty("user", "root")
        prop.setProperty("password", "root")
        sparkSession.sqlContext.read.jdbc(url, table, columnName, lowerBound, upperBound, numPartitions, prop).createOrReplaceTempView("dwd_user_base_tag")
      } else {
        println("加载表:" + tableName(i))
        data
          .option("url", "jdbc:mysql://localhost:3306/ods?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("dbtable", tableName(i))
          .option("user", "root")
          .option("password", "root")
          .load().createOrReplaceTempView("" + tableName(i) + "")
      }
    }

    /**
      * 2.建立新表dadaCache
      */
    /**
      * val sql = "SELECT a.user_sid,a.state,a.result,date(a.req_time) ymd,a.req_time,a.task_id,c.firstreg_cv,if(a.state = 2, 1, NULL) succ_count,a.city_id,b.province,b.city,c.firstreg_placecid,c.place_name,c.chief_cid,c.chief_name,c.channel_type,c.channel_name,a.surname_sid,a.app_cid,d.app,d.sub_app " +
      * "FROM ods_jobdb_mem_shebao a " +
      * "LEFT JOIN ods_shebaodb_cfg_shebao b " +
      * "ON a.city_id = b.city_id " +
      * "LEFT JOIN dwd_user_base_tag c " +
      * "ON a.user_sid = c.user_sid " +
      * "LEFT JOIN ods_zfgjj_cfg_app_type d " +
      * "ON a.app_cid=d.cid "+
      * "WHERE req_time >=date(date_add(now(),INTERVAL -d DAY))"
      */
    /**
      * SELECT * FROM ods_jobdb_mem_shebao
      * WHERE req_time>='2019-02-18 00:50:52'
      */

    val sql = "SELECT a.user_sid,a.state,a.result,date(a.req_time) ymd,a.req_time,a.task_id,c.firstreg_cv,if(a.state = 2, 1, NULL) succ_count,a.city_id,b.province,b.city,c.firstreg_placecid,c.place_name,c.chief_cid,c.chief_name,c.channel_type,c.channel_name,a.surname_sid,a.app_cid,d.app,d.sub_app " +
      "FROM ods_jobdb_mem_shebao a LEFT JOIN ods_shebaodb_cfg_shebao b ON a.city_id = b.city_id LEFT JOIN dwd_user_base_tag c ON a.user_sid = c.user_sid LEFT JOIN ods_zfgjj_cfg_app_type d ON a.app_cid=d.cid "
    //"WHERE req_time>=req_time >='2019-02-21 00:50:52'"
    //创建新表
    val dataSource = sparkSession.sqlContext.sql("" + sql + "").registerTempTable("dataCache")

    //删除除过dm_shebao_query_new、dataCache的其他表
    for (i <- 0 to tableName.length - 1) {
      if (i > 0) {
        sparkSession.sqlContext.dropTempTable("" + tableName(i) + "")
      }
    }

    /**
      * 3.插入数据
      */
    val sql1 = "SELECT first(date(req_time)) ymd,0 type,0 query_type,1 source_id,ifnull(firstreg_cv, '') source,'' type_id,'' type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, firstreg_cv union all SELECT * from dm_shebao_query_new"

    val sql2 = "SELECT first(date(req_time)) ymd,1 type,0 query_type,1 source_id,ifnull(firstreg_cv, '') source,'' type_id,first(province) type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, firstreg_cv,province union all SELECT * from table1"

    val sql3 = "SELECT first(date(req_time)) ymd,2 type,0 query_type,1 source_id,ifnull(firstreg_cv, '') source,province type_id,city type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, firstreg_cv,province,city union all SELECT * from table2"

    val sql4 = "SELECT first(date(req_time)) ymd,3 type,0 query_type,1 source_id,ifnull(firstreg_cv, '') source,ifnull(chief_cid, '') type_id,ifnull(chief_name, '') type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, firstreg_cv, chief_cid, chief_name union all SELECT * from table3"

    val sql5 = "SELECT first(date(req_time)) ymd,4 type,0 query_type,1 source_id,ifnull(firstreg_cv, '') source,ifnull(firstreg_placecid, '') type_id,ifnull(place_name, '') type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, firstreg_cv, firstreg_placecid, place_name union all SELECT * from table4"

    val sql6 = "SELECT first(date(req_time)) ymd,5 type,0 query_type,1 source_id,ifnull(firstreg_cv, '') source,ifnull(channel_type, '') type_id,ifnull(channel_name, '') type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, firstreg_cv, channel_type, channel_name union all SELECT * from table5"

    val sql7 = "SELECT first(date(req_time)) ymd,0 type,0 query_type,2 source_id,ifnull(sub_app, '') source,'' type_id,'' type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, source union all SELECT * from table6"

    val sql8 = "SELECT first(date(req_time)) ymd,1 type,0 query_type,2 source_id,ifnull(sub_app, '') source,'' type_id,province type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, source, province union all SELECT * from table7"

    val sql9 = "SELECT first(date(req_time)) ymd,2 type,0 query_type,2 source_id,ifnull(sub_app, '') source,province type_id,city type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, source, province, city union all SELECT * from table8"

    val sql10 = "SELECT first(date(req_time)) ymd,3 type,0 query_type,2 source_id,ifnull(sub_app, '') source,ifnull(chief_cid, '') type_id,ifnull(chief_name, '') type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, source, chief_cid, chief_name union all SELECT * from table9"

    val sql11 = "SELECT first(date(req_time)) ymd,4 type,0 query_type,2 source_id,ifnull(sub_app, '') source,ifnull(firstreg_placecid, '') type_id,ifnull(place_name, '') type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, source, firstreg_placecid, place_name union all SELECT * from table10"

    val sql12 = "SELECT first(date(req_time)) ymd,5 type,0 query_type,2 source_id,ifnull(sub_app, '') source,ifnull(channel_type, '') type_id,ifnull(channel_name, '') type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, source, channel_type, channel_name union all SELECT * from table11"

    val sql13 = "SELECT first(date(req_time)) ymd,0 type,'' query_type,1 source_id,ifnull(firstreg_cv, '') source,'' type_id,'' type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, query_type, firstreg_cv union all SELECT * from table12"

    val sql14 = "SELECT first(date(req_time)) ymd,1 type,'' query_type,1 source_id,ifnull(firstreg_cv, '') source,'' type_id,province type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, query_type, firstreg_cv, province union all SELECT * from table13"

    val sql15 = "SELECT first(date(req_time)) ymd,2 type,'' query_type,1 source_id,ifnull(firstreg_cv, '') source,province type_id,city type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, query_type, firstreg_cv, province, city union all SELECT * from table14"

    val sql16 = "SELECT first(date(req_time)) ymd,3 type,'' query_type,1 source_id,ifnull(firstreg_cv, '') source,ifnull(chief_cid, '') type_id,ifnull(chief_name, '') type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, query_type, firstreg_cv, chief_cid, chief_name union all SELECT * from table15"

    val sql17 = "SELECT first(date(req_time)) ymd,4 type,'' query_type,1 source_id,ifnull(firstreg_cv, '') source,ifnull(firstreg_placecid, '') type_id,ifnull(place_name, '') type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, query_type, firstreg_cv, firstreg_placecid, place_name union all SELECT * from table16"

    val sql18 = "SELECT first(date(req_time)) ymd,5 type,'' query_type,1 source_id,ifnull(firstreg_cv, '') source,ifnull(channel_type, '') type_id,ifnull(channel_name, '') type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, query_type, firstreg_cv, channel_type, channel_name union all SELECT * from table17"

    val sql19 = "SELECT first(date(req_time)) ymd,0 type,'' query_type,2 source_id,ifnull(sub_app, '') source,'' type_id,'' type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, query_type, source union all SELECT * from table18"

    val sql20 = "SELECT first(date(req_time)) ymd,1 type,'' query_type,2 source_id,ifnull(sub_app, '') source,'' type_id,province type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, query_type, source, province union all SELECT * from table19"

    val sql21 = "SELECT first(date(req_time)) ymd,2 type,'' query_type,2 source_id,ifnull(sub_app, '') source,province type_id,city type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, query_type, source, province, city union all SELECT * from table20"

    val sql22 = "SELECT first(date(req_time)) ymd,3 type,'' query_type,2 source_id,ifnull(sub_app, '') source,ifnull(chief_cid, '') type_id,ifnull(chief_name, '') type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, query_type, source, chief_cid, chief_name union all SELECT * from table21"

    val sql23 = "SELECT first(date(req_time)) ymd,4 type,'' query_type,2 source_id,ifnull(sub_app, '') source,ifnull(firstreg_placecid, '') type_id,ifnull(place_name, '') type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, query_type, source, firstreg_placecid, place_name union all SELECT * from table22"

    val sql24 = "SELECT first(date(req_time)) ymd,5 type,'' query_type,2 source_id,ifnull(sub_app, '') source,ifnull(channel_type, '') type_id,ifnull(channel_name, '') type_name,count(DISTINCT user_sid) sel_user,count(DISTINCT if(state = 2,user_sid, NULL)) sel_user_suc,round(count(DISTINCT if(state = 2, user_sid, NULL))/count(DISTINCT user_sid) * 100, 2) sel_user_rate,count(DISTINCT task_id) sel_num,count(DISTINCT if(state = 2, task_id, NULL)) sel_num_suc,round(count(DISTINCT if(state = 2, task_id, NULL))/count(DISTINCT task_id) * 100, 2) sel_num_rate,now() update_time " +
      "FROM dataCache GROUP BY ymd, query_type, source, channel_type, channel_name union all SELECT * from table23"

    val sqlArray = Array(sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8, sql9, sql10, sql11, sql12, sql13, sql14, sql15, sql16, sql17, sql18, sql19, sql20, sql21, sql22, sql23, sql24)
    //迭代插入数据
    for (i <- 0 to 23) {
      if (i == 1) {
        println("开始执行第" + (i + 1) + "条sql语句")
        sparkSession.sqlContext.sql("" + sqlArray(i) + "").registerTempTable("table" + (i + 1) + "")
        println("第" + i + "张表已生成")
        sparkSession.sqlContext.dropTempTable("dm_shebao_query_new")
        println("第" + i + "张dm_shebao_query_new表已删除")
      }
      println("开始执行第" + (i + 1) + "条sql语句")
      sparkSession.sqlContext.sql("" + sqlArray(i) + "").registerTempTable("table" + (i + 1) + "")
      println("第" + (i + 1) + "张表已生成")

      sparkSession.sqlContext.dropTempTable("table" + i + "")
      println("第" + i + "张表已删除")
    }
    //删除建立的查询表dataCache,保留终表table24
    sparkSession.sqlContext.dropTempTable("dataCache")

    /**
      * 4.终表数据去重
      */
    println("当前表为：")
    sparkSession.sqlContext.sql("show tables").show()
    println("查询总数为：")
    sparkSession.sqlContext.sql("select count(ymd) from table24").show()
    println("正在查询数据")
    val dataTable = sparkSession.sqlContext.sql("select * from table24").distinct().orderBy("ymd")

    /**
      * 5.将数据导入mysql
      */
    println("开始将数据导入mysql")
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    dataTable.write.jdbc("jdbc:mysql://localhost:3306/dataresult?useUnicode=true&characterEncoding=utf-8", "mynewdemo", prop)
    println("数据导入已完成")

    sparkSession.stop()

  }
}
