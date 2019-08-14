# **clean_hbase_hive_delete.flow**

输入源：hdfs://jianbing/dwh/ods/log/trace/cat-end-logconsume.log.$(date -d '-1 day' +'%Y-%m-%d')

输出：1.日志明细hbase分流 2.登陆用户扩展id统计次数hbase分流 3.日志明细hive分流


业务逻辑
---------------------------
1.clean_data.sh：数据清洗

输入：hdfs://jianbing/dwh/ods/log/trace/cat-end-logconsume.log.$(date -d '-1 day' +'%Y-%m-%d')

输出：hdfs://jianbing/dwh/dw/dwd/log/trace


    #!/bin/sh
    source /etc/profile && spark-submit --master yarn --deploy-mode cluster --class com.jianbing.detail.LogDetail hdfs://jianbing/jars/LogDetail-jar-with-dependencies.jar hdfs://jianbing/dwh/ods/log/trace/cat-end-logconsume.log.$(date -d '-1 day' +'%Y-%m-%d') hdfs://jianbing/dwh/dw/dwd/log/trace

2.load_hbase1.sh：日志明细hbase分流

输入：hdfs://jianbing/dwh/dw/dwd/log/trace

输出：logs:cat_end_detail

    #!/bin/sh
    source /etc/profile && spark-submit --master yarn --deploy-mode cluster --class com.jianbing.main.LogAnalysis LogAnalysis-jar-with-dependencies.jar

3.load_hbase2.sh: extral扩展hbase分流

输入：hdfs://jianbing/dwh/dw/dwd/log/trace

输出：logs:cat_extra_id_times

    #!/bin/sh
    source /etc/profile && spark-submit --master yarn --deploy-mode cluster --class com.jianbing.main.ExtraIdCount ExtraIdCount-jar-with-dependencies.jar

4.load_hive.sh：日志明细hive分流

输入：hdfs://jianbing/dwh/dw/dwd/log/trace

输出：dwd.log_trace

    #!/bin/sh
    source /etc/profile && hive -e "LOAD DATA INPATH 'hdfs://jianbing/dwh/dw/dwd/log/trace/part*' INTO TABLE dwd.log_trace;"


5.delete_path.sh：删除路径hdfs://jianbing/dwh/dw/dwd/log/trace

    #!/bin/sh
    source /etc/profile && hdfs dfs -rm -r hdfs://jianbing/dwh/dw/dwd/log/trace

clean_hbase_hive_delete.flow

    nodes:
      - name: clean_data
        type: command
        config:
          command: sh clean_data.sh
    
      - name: load_hbase1
        type: command
        config:
          command: sh load_hbase1.sh
        dependsOn:
          - clean_data
    
      - name: load_hbase2
        type: command
        config:
          command: sh load_hbase2.sh
        dependsOn:
          - load_hbase1
    
      - name: load_hive
        type: command
        config:
          command: sh load_hive.sh
        dependsOn:
          - load_hbase2
    
      - name: delete_path
        type: command
        config:
          command: sh delete_path.sh
        dependsOn:
          - load_hive

注：
azkaban执行时间：每天早上2：00，已经部署。打包上传azkaban时需要添加：ExtraIdCount-jar-with-dependencies.jar、LogAnalysis-jar-with-dependencies.jar


