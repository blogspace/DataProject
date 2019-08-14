#!/bin/sh
#报表
function Application(){
#执行报表程序
spark-submit --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --executor-memory ${executorMemory} --num-executors ${numberExecutors} --executor-cores ${executorCores} --class com.jianbing.controller.${mainClass} hdfs://jianbing/jars/${jar}-jar-with-dependencies.jar

#数据合并
hadoop fs -test -e ${path}
if [ $? -eq 0 ]; then
hadoop fs -cat ${path1}/part* | hadoop fs -appendToFile - ${path}.txt
fi

#删除目录
hadoop fs -test -e ${path1}/part*
if [ $? -eq 0 ]; then
hdfs dfs -rm -r ${path1}
fi
}
#--num-executors:6~8 --executor-cores:2~3 --driver-memory:4~6g--executor-memory：2~4g
driverMemory=6g
executorMemory=4g
numberExecutors=5
executorCores=2
mainClass=DwoUserBaseInfoController
jar=DwoUserBaseInfo
path=hdfs://jianbing/tao/dwo/zfgjj/dwo_user_base_info
Application