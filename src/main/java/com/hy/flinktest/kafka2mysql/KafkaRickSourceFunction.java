package com.hy.flinktest.kafka2mysql;


import com.hy.flinktest.entity.User;
import com.hy.flinktest.service.ZookeeperService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ClassName: KafkaRickSourceFunction
 * Description: 从kafka获取数据
 *
 * @Author: dengchangshi
 * @Date: 2020/9/24 16:04
 */
@Slf4j
public class KafkaRickSourceFunction extends RichSourceFunction<String> implements CheckpointedFunction {
    //kafka
    private static Properties prop = new Properties();
    private boolean running = true;
    private static ZookeeperService zkService;
    static {
        try {
            zkService = new ZookeeperService();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    /*
    * 一，latest和earliest区别
    1，earliest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
    2，latest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
    提交过offset，latest和earliest没有区别，但是在没有提交offset情况下，用latest直接会导致无法读取旧数据。
    * */
    private static Integer partition = WritedatatoKafka.partition;
    static {
        prop.put("bootstrap.servers",WritedatatoKafka.BROKER_LIST);
        prop.put("zookeeper.connect","localhost:2181");
        prop.put("group.id",WritedatatoKafka.TOPIC_USER);
        prop.put("key.deserializer",WritedatatoKafka.CONST_DESERIALIZER);
        prop.put("value.deserializer",WritedatatoKafka.CONST_DESERIALIZER);
        prop.put("auto.offset.reset","earliest");//earliest，latest
        prop.put("max.poll.records", "100");//每次拉取100条数据
        //prop.put("auto.commit.interval.ms", "1000"); 每秒自动提交一次
        //换成下面这种写法，比较容易看
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");//关闭自动提交
    }

    @Override
    public void run(SourceContext sourceContext) {
        int count = 0;
        //创建一个消费者客户端实例
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(prop);
        //只消费TOPIC_USER 分区
        TopicPartition topicPartition = new TopicPartition(WritedatatoKafka.TOPIC_USER,partition);

        long offset = 0;
        String zkTopicPath = zkService.etlAddTaskPath+ "/" + topicPartition.topic() + "/" + topicPartition.partition();
        try {
            zkService.createOffsetNodeIfNotExist(topicPartition.topic(), topicPartition.partition());
            offset = zkService.getOffset(zkTopicPath);//获取偏移量
            log.info("从偏移量:{} 开始消费",offset);
            System.out.println(String.format("从偏移量 %d 开始消费",offset));
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        offset = placeOffsetToBestPosition(kafkaConsumer, offset, topicPartition);


       // final Map<TopicPartition,OffsetAndMetadata> currentOffset = new ConcurrentHashMap<>();

        try {
            while (running) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
                if (records.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> record : records) {
                    //record.offset();
                    //record.key()
                  //  currentOffset.put(new TopicPartition(record.topic(),record.partition()),new OffsetAndMetadata(record.offset(),"metadata"));
                    String value = record.value();
                    sourceContext.collect(value);
                    if (count % 5 == 0) {//每5条提交一次，如果要求实时性很高，可以去掉改判断，进行实时提交
                        kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                            @Override
                            public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                                if(e != null){
                                    log.error("commit failed"+map);//这里异常回调，跟下面catch异常捕获处理机制一样，选择一种就可以了
                                    //System.out.println("commit failed"+map);
                                    //todo something,错误补偿机制
                                }

                            }
                        });
                    }
                    log.info("存储偏移量:{}",record.offset()+1);
                    System.out.println("存储偏移量:"+String.valueOf(record.offset()+1));
                    zkService.saveOffset(zkTopicPath,record.offset()+1);
                    count++;
                }
            }
        }catch (Exception e){
            log.error("提交失败，进行异常处理");
            //kafkaConsumer.commitAsync(currentOffset,null);
            //todo something,错误补偿机制
        }finally {
            try {
                kafkaConsumer.commitSync();//最后进行一次同步提交
            }finally {
                kafkaConsumer.close();
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

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
