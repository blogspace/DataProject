#!/bin/sh
function filemerge(){
#文件合并
source /etc/profile && hadoop fs -cat ${path1}/part* | hadoop fs -appendToFile - ${path1}.txt

source /etc/profile && hadoop fs -cat ${path1}.txt | hadoop fs -appendToFile - ${path2}.txt

#删除路径
hadoop fs -test -e ${path}.txt
if [ $? -eq 0 ]; then
hdfs dfs -rm -r ${path}
fi
}
path1=hdfs://jianbing/tmp/liuhao/DataMerge
path2=hdfs://jianbing/tao/dwo/zfgjj/sys_user
filemerge