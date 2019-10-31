package com.service;

import com.mapper.Items;

import java.util.List;

public interface IUserService {
    /**
     * 新增一个用户
     * @param name
     * @param age
     */
    void insert(int id,String title,String name, String detail);

    /**
     * 根据name删除一个用户高
     * @param name
     */
    void deleteByName(String name);

    /**
     * 获取用户总量
     * @return
     */
    List<Items> getAllUsers();

    /**
     * 删除所有用户
     */
    void deleteAllUsers();
}
