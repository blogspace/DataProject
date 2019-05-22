package com.spider.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

public class downloadPage {
    /**
     * @param url
     * @param encoding
     * @return
     * @Description
     */
    public static String getHtml(String url, String encoding) {
        StringBuffer sb = new StringBuffer();
        BufferedReader bfr = null;
        //1.根据网址和网页编码获取网页源代码
        //1.1定义网址和编码
        try {
            //1.2建立连接
            URL objUrl = new URL(url);
            //1.3打开连接
            URLConnection uc = objUrl.openConnection();
            //1.4创建文件输入流 建立管道
            //InputStream isr = uc.getInputStream();
            //1.5建立缓冲流-->创建转换流
            bfr = new BufferedReader(new InputStreamReader(uc.getInputStream(), encoding));
            //1.6通过缓冲流读取网页源代码，一次读取一行
            //内容 !=null bfr.readLine || 字节数 !=-1 bfr.read
            String line = "";
            while ((line = bfr.readLine()) != null) {
                sb.append(line).append("\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bfr != null) {
                try {
                    bfr.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return sb.toString();
    }

}
