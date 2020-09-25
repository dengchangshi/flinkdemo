package com.hc.flink


import org.apache.flink.api.scala._

/**
 * 1  *ClassName: BatchWordCountScala
 * 2  * Description: 统计单词,批量统计(Scala实现)
 * 3 * @Author: dengchangshi
 * 4 * @Date: 2020/9/23 12:20
 * 5 */
object BatchWordCountScala {
  def main(args: Array[String]): Unit = {
    val input = "d://tmpdata/test1.txt"
    val output ="d://tmpdata/res333"
    //初始化环境
    val environment = ExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    environment.setParallelism(1);

    //读取数据
    val data: DataSet[String] = environment.readTextFile(input)

    //计算
    val res: AggregateDataSet[(String, Int)] = data.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)

    res.writeAsCsv(output)
    //res.writeAsText(output)
    environment.execute("batchWordCountScala")


  }
}
