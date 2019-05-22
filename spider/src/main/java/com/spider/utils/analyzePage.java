package com.spider.utils;

import org.apache.commons.httpclient.HttpException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileWriter;
import java.io.IOException;

public class analyzePage {
    public  void analyzePage(String path) throws HttpException, IOException {
        String filename = path.substring(path.lastIndexOf('/') + 1).substring(0, path.substring(path.lastIndexOf('/') + 1).length() - 5);
        // 创建字符输出流类对象和已存在的文件相关联。文件不存在的话，并创建。
        FileWriter writer = new FileWriter(filename + ".txt");
        // 读取网页
        Document doc = Jsoup.connect(path).get();

        String movieTitle = doc.getElementById("movieTitle").text();
        writer.write("movieTitle：" + movieTitle);
        writer.write("\r\n");
        writer.write("***************************\r\n");

        // 获取电影相关信息
        String info = doc.getElementById("info").text();
        writer.write(info);
        writer.write("\r\n");
        writer.write("***************************\r\n");

        // 获取电影评分
        String score = doc.getElementById("rating_num").text();
        writer.write("score：" + score);
        writer.write("\r\n");
        writer.write("***************************\r\n");

        // 获取电影评价
        Elements container = doc.getElementsByClass("article");
        Document containerDoc = Jsoup.parse(container.toString());
        Element comment = containerDoc.getElementById("comments-section");
        Document commentDoc = Jsoup.parse(comment.toString());
        Elements cmtslist = commentDoc.getElementsByClass("comment-item");
        for (Element clearfix : cmtslist) {
            // 获取评论人
            String discussant = clearfix.getElementsByClass("comment-info").text();
            writer.write("discussant：" + discussant);
            writer.write("\r\n");
            // 获取评论时间
            String time = clearfix.getElementsByClass("comment-time").text();
            writer.write("comment-time：" + time);
            writer.write("\r\n");
            // 获取评论内容
            String commentContent = clearfix.select("p").get(0).text();
            writer.write("comment-content：" + commentContent);
            writer.write("\r\n");
            writer.write("====================================\r\n");
        }
        // 刷新该流中的缓冲。将缓冲区中的字符数据保存到目的文件中去。
        writer.flush();
        // 关闭此流
        writer.close();
    }
}
