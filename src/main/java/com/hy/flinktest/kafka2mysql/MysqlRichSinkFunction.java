package com.hy.flinktest.kafka2mysql;

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
public class MysqlRichSinkFunction extends RichSinkFunction<List<User>> {

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

    public void invoke(List<User> users, Context ctx) throws Exception {
        //获取ReadMysqlResoure发送过来的结果
        for(User user : users) {
            ps.setLong(1, user.getId());
            ps.setString(2, user.getName());
            ps.addBatch();
        }
        //一次性写入
        int[] count = ps.executeBatch();
        log.info("成功写入Mysql数量：" + count.length);

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
