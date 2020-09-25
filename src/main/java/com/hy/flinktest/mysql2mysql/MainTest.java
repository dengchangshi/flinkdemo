package com.hy.flinktest.mysql2mysql;

import com.hy.flinktest.entity.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;


/**
 * ClassName: MainTest
 * Description: 用于测试
 *
 * @Author: dengchangshi
 * @Date: 2020/9/24 10:36
 */
@Slf4j
public class MainTest {
    @Test
    //第一种方式，通过Tuple
    public void testMysqltoMysqlbyTuple() {
        //初始化环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每隔3s进行启动一个检查点
        environment.enableCheckpointing(3000);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置模式为exactly-once （这是默认值）
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //获取数据
        DataStreamSource<Tuple2<String, String>> data = environment.addSource(new ReadMysqlResoureTuple());
        //写入mysql
        data.addSink(new WriteMysqlSinkTuple());

        try {
            log.info("开始执行任务了");
            environment.execute("开始执行任务:testMysqltoMysqlbyTuple");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("runException:{}",e);
        }
    }


    @Test
    //第二种方式写mysql，通过实体对象
    public void testMysqltoMysqlbyClass() {
        //初始化环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 每隔3s进行启动一个检查点
        environment.enableCheckpointing(3000);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置模式为exactly-once （这是默认值）
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //获取数据
        DataStreamSource<User> data = environment.addSource(new ReadMysqlResoureClass());
        //写入mysql
        data.addSink(new WriteMysqlSinkClass());
        try {
            log.info("开始执行任务了{}","dengcs");
            environment.execute("开始执行任务:testMysqltoMysqlbyClass");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("runException:{}",e);
        }
    }

}
