#!/bin/sh
#报表
function application(){
spark-submit --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --executor-memory ${executorMemory} --num-executors ${numberExecutors} --executor-cores ${executorCores} --class com.jianbing.controller.${mainClass} hdfs://jianbing/jars/${jar}-jar-with-dependencies.jar ${path}
}
driverMemory=4g
executorMemory=3g
numberExecutors=5
executorCores=2
mainClass=DimOrderOptController
jar=DimOrderOpt
#path=webhdfs://jianbing/tao/dim/zfgjj/dim_order_opt
path=webhdfs://jianbing/tmp/liuhao/dim_order_opt
application