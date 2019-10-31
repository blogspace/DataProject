package com.controller;


import com.properties.HbaseConfig;
import com.properties.HbaseProperties;
import com.service.impl.HbaseServiceImpl;
import org.apache.hadoop.hbase.client.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class HbaseController {

    @Autowired
    private HbaseServiceImpl hbaseService;

    @RequestMapping("/demotest")
    public List<Result> demo() {
        return hbaseService.scaner("log:logClean");
    }

}
