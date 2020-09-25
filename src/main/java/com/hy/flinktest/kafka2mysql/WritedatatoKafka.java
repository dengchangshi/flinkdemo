package com.hy.flinktest.kafka2mysql;

import com.alibaba.fastjson.JSON;
import com.hy.flinktest.entity.User;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * ClassName: WritedatatoKafka
 * Description: 写数据到kafka中
 *
 * @Author: dengchangshi
 * @Date: 2020/9/24 15:36
 */
public class WritedatatoKafka {
    //本地的kafka机器列表
    public static final String BROKER_LIST = "172.18.0.210:9092";
    //kafka的topic
    public static final String TOPIC_USER = "USER";
    //kafka的partition分区
    public static final Integer partition = 0;

    //序列化的方式
    public static final String CONST_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    //反序列化
    public static final String CONST_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";




    public static void writeToKafka() throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", CONST_SERIALIZER);
        props.put("value.serializer", CONST_SERIALIZER);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //构建User对象，在name为hyzs后边加个随机数
        int randomInt = RandomUtils.nextInt(1, 100000);
        User user = new User();
        user.setName("hyzs" + randomInt);
        user.setId(randomInt);
        //转换成JSON
        String userJson = JSON.toJSONString(user);

        //包装成kafka发送的记录
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_USER, partition,
                null, userJson);
        //发送到缓存
        producer.send(record);
        System.out.println("向kafka发送数据:" + userJson);
        //立即发送
        producer.flush();

    }

    public static void main(String[] args) {
        while(true) {
            try {
                //每三秒写一条数据
                TimeUnit.SECONDS.sleep(3);
                writeToKafka();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }



}
