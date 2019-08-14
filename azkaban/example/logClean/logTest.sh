#!/bin/sh
#数据清洗
function Application(){
hadoop fs -test -e ${src}
if [ $? -eq 0 ]; then
spark-submit --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --executor-memory ${executorMemory} --num-executors ${numberExecutors} --executor-cores ${executorCores} --class com.jianbing.mains.${mainClass} hdfs://jianbing/jars/${jar} ${src} ${drc}
fi

#数据合并
hadoop fs -test -e ${drc}
if [ $? -eq 0 ]; then
hadoop fs -cat ${drc}/part* | hadoop fs -appendToFile - ${drc}/cat-end-logconsume-clean.$(date -d '-1 day' +'%Y-%m-%d')
fi

#导入hive
hadoop fs -test -e ${drc}/cat-end-logconsume-clean.$(date -d '-1 day' +'%Y-%m-%d')
if [ $? -eq 0 ]; then
ssh gaiad "
source /etc/profile && hive -e \"LOAD DATA INPATH 'hdfs://jianbing/dwh/dw/dwd/log/trace/cat-end-logconsume-clean.$(date -d '-1 day' +'%Y-%m-%d')' INTO TABLE default.log_trace;\"
"
fi

#删除目录
hadoop fs -test -e ${drc}/part*
if [ $? -eq 0 ]; then
hdfs dfs -rm -r hdfs://jianbing/dwh/dw/dwd/log/trace
fi
}
src=hdfs://jianbing/dwh/ods/log/trace/log/cat-end-logconsume.log.$(date -d '-1 day' +'%Y-%m-%d')
drc=hdfs://jianbing/dwh/dw/dwd/log/trace
driverMemory=2g
numberExecutors=2
executorMemory=4g
executorCores=5
mainClass=LogDetail
jar=LogDetail-jar-with-dependencies.jar
Application