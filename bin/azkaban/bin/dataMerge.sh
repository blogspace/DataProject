#!/bin/sh
function datamerge(){
#文件合并
source /etc/profile && hadoop fs -cat ${path}/part* | hadoop fs -appendToFile - ${path}.txt

#删除路径
hadoop fs -test -e ${path}.txt
if [ $? -eq 0 ]; then
hdfs dfs -rm -r ${path}
fi
}
path=hdfs://jianbing/tao/dim/user/first_login_info/first_login_info
#path=hdfs://jianbing/tao-dev/dim/user/first_login_info/first_login_info
datamerge