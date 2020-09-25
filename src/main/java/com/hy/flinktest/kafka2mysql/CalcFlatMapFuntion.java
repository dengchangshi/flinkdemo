package com.hy.flinktest.kafka2mysql;

import com.hy.flinktest.entity.User;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.nio.file.attribute.UserDefinedFileAttributeView;

/**
 * ClassName: CalcFlatFuntion
 * Description: 对读取的数据进行加工计算
 *
 * @Author: dengchangshi
 * @Date: 2020/9/25 16:54
 */

//FlatMapFunction<User,User> 第一个User 是入参，第二个User是出参，就是要返回的类型
public class CalcFlatFuntion implements FlatMapFunction<User,User> {
    @Override
    public void flatMap(User user, Collector collector) throws Exception {
        User user1 =  new User();
        user1.setId(user.getId());
        user1.setName(user.getName()+"_test");
        collector.collect(user1);
    }
}
