package com.hy.flinktest.mysql2mysql;

import com.hy.flinktest.entity.User;
import com.hy.flinktest.utils.DbUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;


/**
 * ClassName: WriteMysqlSink
 * Description: 写入目标数据库sink
 *
 * @Author: dengchangshi
 * @Date: 2020/9/24 10:19
 */
@Slf4j
public class WriteMysqlSinkClass extends RichSinkFunction<User> {

    private Connection connection = null;
    private PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
       // super.open(parameters);
        log.info("获取数据库连接");
        connection = DbUtil.getConnection();
        String sql = "insert into user1(id,name) values (?,?)";
        ps = connection.prepareStatement(sql);
    }

    public void invoke(User user, Context ctx) throws Exception {
        //获取ReadMysqlResoure发送过来的结果
        long id = user.getId();
        String name = user.getName();
        ps.setLong(1,id);
        ps.setString(2,name);
        ps.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        //关闭并释放资源
        if(connection != null) {
            connection.close();
        }

        if(ps != null) {
            ps.close();
        }
    }
}
