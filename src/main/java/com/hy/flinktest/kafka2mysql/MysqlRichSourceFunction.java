package com.hy.flinktest.kafka2mysql;

import com.hy.flinktest.entity.User;
import com.hy.flinktest.utils.DbUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import javax.jws.soap.SOAPBinding;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: ReadMysqlResoure
 * Description: 从mysql获取数据resoure
 *
 * @Author: dengchangshi
 * @Date: 2020/9/24 9:33
 */
@Slf4j
public class MysqlRichSourceFunction extends RichSourceFunction<List<User>> {

    private Connection connection = null;
    private PreparedStatement ps = null;


    //@Override
    //该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
    private void open() throws Exception {
        //super.open(parameters);
        log.info("获取数据库连接");
        connection = DbUtil.getConnection();
        ps = connection.prepareStatement("select * from user");
    }

    //@Override
    public void cancel() {
        try {
            if(ps != null){
                ps.close();
            }
            if(connection != null){
                connection.close();
            }
        }catch (SQLException e){
            log.error("runException:{}", e);
        }
    }

    //执行查询并获取结果

   // @Override
    public void run(SourceContext<List<User>> sourceContext) throws Exception {
        open();
        try {
            ResultSet resultSet = ps.executeQuery();
            List<User> users = new ArrayList<>();
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                //String id = resultSet.getString("id");
                long id = resultSet.getInt("id")*10;
                log.error("readJDBC name:{}{}",id, name);
                //Tuple2<String, String> tuple2 = new Tuple2<String, String>();
                //tuple2.setFields(id, name);
                User user = new User(id, name);
                users.add(user);
                sourceContext.collect(users);//发送结果
            }
        }finally {
            cancel();
        }

    }
}
