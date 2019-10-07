#!/bin/sh
#报表
function application(){
spark-submit --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --executor-memory ${executorMemory} --num-executors ${numberExecutors} --executor-cores ${executorCores} --class com.jianbing.controller.${mainClass} hdfs://jianbing/jars/${jar}-jar-with-dependencies.jar ${path}
}
driverMemory=4g
executorMemory=3g
numberExecutors=5
executorCores=2
mainClass=DwcOrderUserBaseInfoController
jar=DwcOrderUserBaseInfo
#path=webhdfs://jianbing/tao/dwc/dwc_order_user_base_info
path=webhdfs://jianbing/tmp/liuhao/dwc_order_user_base_info
application