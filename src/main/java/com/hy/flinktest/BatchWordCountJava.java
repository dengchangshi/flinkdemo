package com.hy.flinktest;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

/**
 * ClassName: BatchWordCountJava
 * Description: 统计单词,批量统计(java实现)
 * Date: 2020/9/23 10:10
 *
 * @Author dengchangshi
 */
public class BatchWordCountJava {
    public static void main(String[] args) {
        String input = "d://tmpdata/test1.txt";
        String output = "d://tmpdata/result";
        //初始化环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //读取数据
        DataSource<String> data = environment.readTextFile(input);

        AggregateOperator<Tuple2<String, Long>> res = data.flatMap(new SplitFunction()).groupBy(0).sum(1);
        res.writeAsCsv(output, FileSystem.WriteMode.OVERWRITE);
        //res.writeAsText(output, FileSystem.WriteMode.OVERWRITE);
        try {
            environment.execute("BatchWordCountJava");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static class SplitFunction implements FlatMapFunction<String , Tuple2<String,Long>>{
        public void flatMap(String value, Collector<Tuple2<String,Long>> collector) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                if(StringUtils.isNotBlank(word)){
                    collector.collect(new Tuple2<String, Long>(word,1L));
                }
            }

        }
    }


}
