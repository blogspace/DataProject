#!/bin/sh
function filemerge(){
#文件合并
source /etc/profile && hadoop fs -cat ${path}/part* | hadoop fs -appendToFile - ${path}.txt

#删除路径
hadoop fs -test -e ${path}.txt
if [ $? -eq 0 ]; then
hdfs dfs -rm -r ${path}
fi
}
#path=hdfs://jianbing/tao/dim/zfgjj/dim_user_app_opt
path=webhdfs://jianbing/tmp/liuhao/dim_user_app_opt
filemerge