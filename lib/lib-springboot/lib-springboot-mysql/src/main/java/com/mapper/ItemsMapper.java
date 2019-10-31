package com.mapper;

import org.springframework.jdbc.core.RowMapper;

import javax.swing.tree.TreePath;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ItemsMapper implements RowMapper<Items> {
    @Override
    public Items mapRow(ResultSet resultSet, int i) throws SQLException {
        Items items = new Items();
        items.setId(resultSet.getInt("id"));
        items.setTitle(resultSet.getString("title"));
        items.setName(resultSet.getString("name"));
        items.setDetail(resultSet.getString("detail"));
        return items;
    }
}
