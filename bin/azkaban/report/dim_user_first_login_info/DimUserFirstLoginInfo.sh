#!/bin/sh
#报表
function application(){
spark-submit --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --executor-memory ${executorMemory} --num-executors ${numberExecutors} --executor-cores ${executorCores} --class com.jianbing.controller.${mainClass} hdfs://jianbing/jars/${jar}-jar-with-dependencies.jar ${path}
}
driverMemory=5g
executorMemory=5g
numberExecutors=3
executorCores=2
parallelism=150
mainClass=DimUserFirstLoginInfoController
jar=DimUserFirstLoginInfo
#path=webhdfs://jianbing/tao/dim/zfgjj/dim_user_first_login_info
path=webhdfs://jianbing/tmp/liuhao/dim_user_first_login_info
application