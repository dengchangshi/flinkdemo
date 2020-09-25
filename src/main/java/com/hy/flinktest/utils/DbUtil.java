package com.hy.flinktest.utils;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;

/**
 * ClassName: DbUtil
 * Description: 获取数据库连接
 *
 * @Author: dengchangshi
 * @Date: 2020/9/24 9:55
 */
public class DbUtil {
    private static DruidDataSource dataSource;

    public static Connection getConnection() throws Exception {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        //dataSource.setUrl("jdbc:mysql://localhost:3306/testdb");
        dataSource.setUrl("jdbc:mysql://localhost:3306/testdb?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf8&autoReconnect=true&useSSL=false");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        //设置初始化连接数，最大连接数，最小闲置数
        dataSource.setInitialSize(10);
        dataSource.setMaxActive(50);
        dataSource.setMinIdle(5);
        //返回连接
        return  dataSource.getConnection();
    }
}
