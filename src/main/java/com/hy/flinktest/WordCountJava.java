package com.hy.flinktest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

/**
 * ClassName: WorkCountJava
 * Description: 统计单词,通过窗口，每间隔1秒对近2秒的数据进行操作(java实现)
 * Date: 2020/9/22 14:47
 *
 * @Author dengchangshi
 */
public class WordCountJava {
    public static void main(String[] args) {

        //获取端口号
        int port=0;
        try {
            ParameterTool tool = ParameterTool.fromArgs(args);
            port = tool.getInt("port");
        }catch (Exception e){
            System.err.println("no set port! default use 6666");
            port = 6666;
        }

        String hostName = "david";

        //初始化环境对象
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        environment.setParallelism(1);
        //获取数据
        DataStreamSource<String> data = environment.socketTextStream(hostName, port);
        //开始计算
        SingleOutputStreamOperator<WordwithCount> pairwords = data.flatMap(new FlatMapFunction<String, WordwithCount>() {
            public void flatMap(String s, Collector<WordwithCount> collector) throws Exception {
                String[] splits = s.split(" ");
                for (String word : splits) {
                    collector.collect(new WordwithCount(word, 1L));
                }
            }
        });
        //将元素按key进行分组
        KeyedStream<WordwithCount, Tuple> grouped = pairwords.keyBy("word");
        //调用窗口操作
        WindowedStream<WordwithCount, Tuple, TimeWindow> window = grouped.timeWindow(Time.seconds(2), Time.seconds(1));

        //第一种写法
        SingleOutputStreamOperator<WordwithCount> count = window.sum("count");
        //第二种写法
        /*
        window.reduce(new ReduceFunction<WordwithCount>() {
            public WordwithCount reduce(WordwithCount t1, WordwithCount t2) throws Exception {
                return new WordwithCount(t1.word,t1.count+t2.count);
            }
        });

        */

        //打印，并设置
        count.print().setParallelism(1);

        try {
            environment.execute("java wordCount");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static class WordwithCount{
        private String word;
        private long count;

        public WordwithCount(){
            this.word = "ddd";
            this.count = 1;
        }
        public WordwithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordwithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
