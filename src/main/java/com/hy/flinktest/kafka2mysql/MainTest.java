package com.hy.flinktest.kafka2mysql;

import com.alibaba.fastjson.JSONObject;
import com.hy.flinktest.entity.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.List;


/**
 * ClassName: MainTest
 * Description: 用于测试
 *
 * @Author: dengchangshi
 * @Date: 2020/9/24 10:36
 */
@Slf4j
public class MainTest {

    //测试从mysql读取数据写入mysql
    @Test
    public void testMysqltoMysqlbyListClass() {
        //初始化环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 每隔3s进行启动一个检查点
        environment.enableCheckpointing(3000);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置模式为exactly-once （这是默认值）
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //获取数据
        DataStreamSource<List<User>> data = environment.addSource(new MysqlRichSourceFunction());
        //写入mysql
        data.addSink(new MysqlRichSinkFunction());
        try {
            log.info("开始执行任务了{}","dengcs");
            environment.execute("开始执行任务:testMysqltoMysqlbyClass");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("runException:{}",e);
        }
    }

    //测试从kafka读取数据写入mysql
    @Test
    public void testFromkafka() throws Exception {
        //构建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.addSource(new KafkaRickSourceFunction());

        //从kafka里读取数据，转换成User对象
        DataStream<User> dataStream = dataStreamSource.map(lines -> JSONObject.parseObject(lines, User.class)).flatMap(new CalcFlatFuntion());
        //收集5秒钟的总数
        dataStream.timeWindowAll(Time.seconds(5L)).
                apply(new AllWindowFunction<User, List<User>, TimeWindow>() {

                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<User> iterable, Collector<List<User>> out) throws Exception {
                        List<User> users = Lists.newArrayList(iterable);

                        if(users.size() > 0) {
                            System.out.println("5秒的总共收到的条数：" + users.size());
                            out.collect(users);
                        }

                    }
                })
                //sink 到数据库
                .addSink(new MysqlRichSinkFunction());
        //打印到控制台
        //.print();


        env.execute("kafka 消费任务开始");
    }
}
