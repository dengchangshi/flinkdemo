package com.hy.flinktest.mysql2mysql;

import com.hy.flinktest.utils.DbUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;


/**
 * ClassName: WriteMysqlSink
 * Description: 写入目标数据库sink
 *
 * @Author: dengchangshi
 * @Date: 2020/9/24 10:19
 */
@Slf4j
public class WriteMysqlSinkTuple extends RichSinkFunction<Tuple2<String,String>> {

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

    public void invoke(Tuple2<String, String> value, Context ctx) throws Exception {
        try {
            //获取ReadMysqlResoure发送过来的结果
            int id = Integer.valueOf(value.f0);
            String name = value.f1;
            ps.setInt(1,id);
            ps.setString(2,name);
            ps.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
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
