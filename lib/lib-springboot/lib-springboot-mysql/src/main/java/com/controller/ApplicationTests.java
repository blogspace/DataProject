package com.controller;

import com.mapper.Items;
import com.service.IUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class ApplicationTests {
    @Autowired
    private IUserService userSerivce;

    public void setUp() {
        // 准备，清空user表
        userSerivce.deleteAllUsers();
    }

    @RequestMapping("/insertDemo")
    public void test() throws Exception {
        // 插入5个用户
        userSerivce.insert(1, "demo", "小明", "开发人员");
        userSerivce.insert(2, "test", "小汪", "开发人员");
        userSerivce.insert(3, "demo", "小花", "测试人员");
        userSerivce.insert(4, "prod", "小张", "测试人员");
        userSerivce.insert(5, "prod", "小李", "大数据");

        userSerivce.getAllUsers();
    }

    @RequestMapping("/demotest")
    public List<Items> demo() {
        return userSerivce.getAllUsers();
    }
}
