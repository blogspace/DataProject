package com.spider.mains;

import com.spider.utils.downloadPage;

public class DataTest {
    public static void main(String[] args) {
        String url = "https://movie.douban.com/subject/4739952/comments?status=P";
        String encoding = "utf-8";
        String html = downloadPage.getHtml(url, encoding);
        System.out.println(html);
    }
}
