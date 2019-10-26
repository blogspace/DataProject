#!/bin/sh
#报表
function application(){
#删除路径
hadoop fs -test -e ${path}*
if [ $? -eq 0 ]; then
hdfs dfs -rm -r ${path}*
fi
#报表
spark-submit --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --executor-memory ${executorMemory} --num-executors ${numberExecutors} --executor-cores ${executorCores} --conf spark.default.parallelism=${parallelism} --conf spark.storage.memoryFraction=${storageMemoryFraction} --conf spark.shuffle.memoryFraction=${shuffleMemoryFraction} --class com.jianbing.controller.${mainClass} hdfs://jianbing/jars/${jar}-jar-with-dependencies.jar ${path}
}
driverMemory=20g
executorMemory=20g
numberExecutors=5
executorCores=2
parallelism=350
storageMemoryFraction=0.5
shuffleMemoryFraction=0.3
mainClass=DemoClass
jar=DemoClass
path=webhdfs://jianbing/tao/dim/user/first_login_info
application
 #--master yarn-cluster \
 #--num-executors 100 \
 #--executor-memory 6G \
 #--executor-cores 4 \
 #--driver-memory 1G \
 #--conf spark.default.parallelism=1000 \
 #--conf spark.storage.memoryFraction=0.5 \
 #--conf spark.shuffle.memoryFraction=0.3 \