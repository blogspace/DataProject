#!/bin/sh
source /etc/profile && hadoop fs -cat hdfs://jianbing/dwh/dw/dwd/log/trace/* | hadoop fs -appendToFile - hdfs://jianbing/dwh/dw/dwd/log/trace/logClean.txt