# **clean_hbase_hive_delete.flow**

输入源：hdfs://jianbing/dwh/ods/log/trace/cat-end-logconsume.log.$(date -d '-1 day' +'%Y-%m-%d')

输出：1.日志明细hbase分流 2.登陆用户扩展id统计次数hbase分流 3.日志明细hive分流


业务逻辑
---------------------------
1.clean_data.sh：数据清洗
输入：hdfs://jianbing/dwh/ods/log/trace/cat-end-logconsume.log.$(date -d '-1 day' +'%Y-%m-%d')
输出：hdfs://jianbing/dwh/dw/dwd/log/trace

2.load_hbase1.sh：日志明细hbase分流
输入：hdfs://jianbing/dwh/dw/dwd/log/trace
输出：logs:cat_end_detail

3.load_hbase2.sh: extral扩展hbase分流
输入：hdfs://jianbing/dwh/dw/dwd/log/trace
输出：logs:cat_extra_id_times

4.load_hive.sh：日志明细hive分流
输入：hdfs://jianbing/dwh/dw/dwd/log/trace
输出：dwd.log_trace

5.delete_path.sh：删除路径hdfs://jianbing/dwh/dw/dwd/log/trace

azkaban执行时间：每天早上2：00


