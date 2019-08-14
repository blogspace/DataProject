#!/bin/sh
#报表
function application(){
#删除路径
hadoop fs -test -e ${path}*
if [ $? -eq 0 ]; then
hdfs dfs -rm -r ${path}*
fi
#报表
spark-submit --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --executor-memory ${executorMemory} --num-executors ${numberExecutors} --executor-cores ${executorCores}  --conf spark.default.parallelism=${parallelism} --class com.jianbing.controller.${mainClass} hdfs://jianbing/jars/${jar}-jar-with-dependencies.jar ${path}
#spark-submit --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 10g --driver-cores 2 --executor-cores 5 --class com.jianbing.controller.${mainClass} hdfs://jianbing/jars/${jar}-jar-with-dependencies.jar ${path}

}
#--num-executors:6~8 --executor-cores:2~3 --driver-memory:4~6g--executor-memory：2~4g
driverMemory=6g
executorMemory=6g
numberExecutors=4
executorCores=2
parallelism=150
mainClass=UserFirstLoginInfoController
jar=userfirstlogininfo
#--conf spark.network.timeout=10000000
#path=webhdfs://jianbing/tao/dim/user/first_login_info/first_login_info
path=webhdfs://jianbing/tao-dev/dim/user/first_login_info/first_login_info
application
