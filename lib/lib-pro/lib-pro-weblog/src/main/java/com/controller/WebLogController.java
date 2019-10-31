package com.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import java.util.Locale;

@RestController
public class WebLogController {
    @RequestMapping("/frist")
    public ModelAndView firstDemo() {
        return new ModelAndView("test");
    }

    @RequestMapping("/user")
    public String index(Model md) {
        md.addAttribute("driverMemory", "20G");
        md.addAttribute("executorMemory", "10G");
        md.addAttribute("mainClass", "Hello");
        return "miao";
    }

    @RequestMapping("/miaotest")
    public ModelAndView secondDemo() {
        ModelAndView model = new ModelAndView();
        model.setViewName("miao");
        model.addObject("driverMemory", "3G");
        model.addObject("executorMemory", "2G");
        model.addObject("mainClass", "Hello!");
        return model;
    }

    @RequestMapping("/helloworld")
    public void test(@PathVariable(name = "id") String id, @RequestParam(name = "name") String name) {
        System.out.println("id="+id);
        System.out.println("name="+name);
    }


}
