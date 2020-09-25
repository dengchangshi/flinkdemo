package com.hy.flinktest.entity;

import java.io.Serializable;

/**
 * ClassName: user
 * Description: 用户实体类
 *
 * @Author: dengchangshi
 * @Date: 2020/9/24 12:01
 */
public class User{
    private long id;
    private String name;

    public User() {
        this.id = 0;
        this.name = "name";
    }

    public User(long id, String name) {
        this.id = id;
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
