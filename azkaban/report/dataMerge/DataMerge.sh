#!/bin/sh
#报表
function application(){
#删除路径
hadoop fs -test -e ${path}*
if [ $? -eq 0 ]; then
hdfs dfs -rm -r ${path}*
fi
#报表
spark-submit --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --executor-memory ${executorMemory} --num-executors ${numberExecutors} --executor-cores ${executorCores} --class com.jianbing.controller.${mainClass} hdfs://jianbing/jars/${jar}-jar-with-dependencies.jar ${path}
}
driverMemory=4g
executorMemory=3g
numberExecutors=3
executorCores=2
mainClass=DataMerge
jar=DataMerge
path=webhdfs://jianbing/tmp/liuhao/DataMerge
application