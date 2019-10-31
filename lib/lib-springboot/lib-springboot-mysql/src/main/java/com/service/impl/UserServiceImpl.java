package com.service.impl;

import com.mapper.Items;
import com.service.IUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class UserServiceImpl  implements IUserService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public void insert(int id,String title,String name, String detail) {
        jdbcTemplate.update("insert into items(id, title,name,detail) values(?, ?,?,?)", id, title,name,detail);
    }

    @Override
    public void deleteByName(String name) {
        jdbcTemplate.update("delete from items where NAME = ?", name);
    }

    @Override
    public List<Items> getAllUsers() {
        List<Items> items = new ArrayList<>();
        jdbcTemplate.queryForList("select * from items").forEach(ele->{
            Items ite = new Items();
            ite.setId((int)ele.get("id"));
            ite.setTitle((String) ele.get("title"));
            ite.setName((String) ele.get("name"));
            ite.setDetail((String) ele.get("detail"));
          items.add(ite);
        });
        return items;

    }

    @Override
    public void deleteAllUsers() {
        jdbcTemplate.update("delete from items");
    }
}
