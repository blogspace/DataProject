#!/bin/sh
ssh gaiad "
source /etc/profile && hive -e \"LOAD DATA INPATH 'hdfs://jianbing/dwh/dw/dwd/log/trace/cat-end-logconsume-clean.$(date -d '-1 day' +'%Y-%m-%d')' INTO TABLE dwd.log_trace;\"
"