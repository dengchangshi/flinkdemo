package com.hy.flinktest.kafka2mysql;


import com.hy.flinktest.entity.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * ClassName: KafkaRickSourceFunction
 * Description: 从kafka获取数据
 *
 * @Author: dengchangshi
 * @Date: 2020/9/24 16:04
 */
@Slf4j
public class KafkaRickSourceFunction extends RichSourceFunction<String>{
    //kafka
    private static Properties prop = new Properties();
    private boolean running = true;


    private static Integer partition = WritedatatoKafka.partition;
    static {
        prop.put("bootstrap.servers",WritedatatoKafka.BROKER_LIST);
        prop.put("zookeeper.connect","localhost:2181");
        prop.put("group.id",WritedatatoKafka.TOPIC_USER);
        prop.put("key.deserializer",WritedatatoKafka.CONST_DESERIALIZER);
        prop.put("value.deserializer",WritedatatoKafka.CONST_DESERIALIZER);
        prop.put("auto.offset.reset","latest");
        prop.put("max.poll.records", "500");
        prop.put("auto.commit.interval.ms", "1000");
    }

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        //创建一个消费者客户端实例
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(prop);
        //只消费TOPIC_USER 分区
        TopicPartition topicPartition = new TopicPartition(WritedatatoKafka.TOPIC_USER,partition);
        long offset =0; //这个初始值应该从zk或其他地方获取
        offset = placeOffsetToBestPosition(kafkaConsumer, offset, topicPartition);


        while (running){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            if(records.isEmpty()){
                continue;
            }
            for (ConsumerRecord<String, String> record : records) {
                //record.offset();
                //record.key()
                String value = record.value();
                sourceContext.collect(value);
            }
        }

    }

    /**
     * 将offset定位到最合适的位置，并返回最合适的offset。
     * @param kafkaConsumer consumer
     * @param offset offset
     * @param topicPartition partition
     * @return the best offset
     */
    private long placeOffsetToBestPosition(
            KafkaConsumer<String, String> kafkaConsumer,
            long offset, TopicPartition topicPartition) {
        List<TopicPartition> partitions = Collections.singletonList(topicPartition);
        kafkaConsumer.assign(partitions);
        long bestOffset = offset;
        if (offset == 0) {
            log.info("由于offset为0，重新定位offset到kafka起始位置.");
            kafkaConsumer.seekToBeginning(partitions);

        } else if (offset > 0) {

            kafkaConsumer.seekToBeginning(partitions);
            long startPosition = kafkaConsumer.position(topicPartition);
            kafkaConsumer.seekToEnd(partitions);
            long endPosition = kafkaConsumer.position(topicPartition);

            if (offset < startPosition) {
                log.info("由于当前offset({})比kafka的最小offset({})还要小，则定位到kafka的最小offset({})处。",
                        offset, startPosition, startPosition);
                kafkaConsumer.seekToBeginning(partitions);
                bestOffset = startPosition;
            } else if (offset > endPosition) {
                log.info("由于当前offset({})比kafka的最大offset({})还要大，则定位到kafka的最大offset({})处。",
                        offset, endPosition, endPosition);
                kafkaConsumer.seekToEnd(partitions);
                bestOffset = endPosition;
            } else {
                kafkaConsumer.seek(topicPartition, offset);
            }
        }
        return bestOffset;
    }

    @Override
    public void cancel() {
        running = false;
    }

}
