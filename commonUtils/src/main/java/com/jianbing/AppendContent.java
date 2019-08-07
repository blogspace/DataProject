package com.jianbing;

import com.jianbing.utils.DateUtils;


public class AppendContent {
    public static void main(String[] args) {

//        String hdfs_path = "hdfs://ddx:9000/data/log_spark/devgjj_logdetail/part-00000";//文件路径
//        String inpath = "D:\\admin\\Desktop\\part-00001";
//        HdfsUtil.AppendContent(inpath,hdfs_path);
        System.out.println(DateUtils.lastDate(0));
    }
}
