#!/bin/sh
ssh gaiad "
source /etc/profile && hive -e \"LOAD DATA INPATH 'hdfs://jianbing/dwh/dw/dwd/log/trace/part*' INTO TABLE dwd.log_trace;\"
"