# **spark改写存储过程**

1.数据导入：使用spark sql读取mysql数据库中的5张表并将每个DataFrame注册成为TempTable，分别为：dm_shebao_query_new、dwd_user_base_tag、ods_jobdb_mem_shebao、ods_shebaodb_cfg_shebao、ods_zfgjj_cfg_app_type

2.查询表创建：基于之前创建的TempTable使用Spark Sql执行存储过程的第一条sql语句，创建出一张新表dataCache,方便后续查询数据使用，该表功能与存储过程中的“dwd.tmp_dwd_shebao_query_new_copy_zn”相同。删除除过dm_shebao_query_new、dataCache的其他表

3.迭代插入数据：改写存储过程的sql语句，使用union all操作对查询得到的两个DataFrame进行组合并注册成为一张新表，用table*表示。每生成一张新表table(i)，就删除掉一张table(i-1),留下最后一张表table24作为终表并注册为新表

4.终表数据去重、排序：调用DataFrame的去重算子distinct对DataFrame进行去重并排序。

5.数据导出：将数据写入mysql



