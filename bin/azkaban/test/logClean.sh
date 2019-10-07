#!/bin/sh
function logClean(){
hadoop fs -test -e ${src}
if [ $? -eq 0 ]; then
source /etc/profile && spark-submit --master yarn --deploy-mode cluster --class com.jianbing.mains.LogDetail hdfs://jianbing/jars/LogDetail-jar-with-dependencies.jar ${src} ${drc}
fi
}
src = hdfs://jianbing/dwh/ods/log/trace/log/cat-end-logconsume.log.$(date -d '-1 day' +'%Y-%m-%d')
drc = hdfs://jianbing/dwh/dw/dwd/log/trace


