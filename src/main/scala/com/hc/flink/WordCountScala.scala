package com.hc.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * ClassName: WordCountScala
 * Description: 统计单词,通过窗口，每间隔1秒对近2秒的数据进行操作(scala实现)
 * Date: 2020/9/22 16:57
 *
 * @Author dengchangshi
 */
object WordCountScala {
  def main(args: Array[String]): Unit = {

    //获取netcat的port
    var port = 0
    try {
      port = ParameterTool.fromArgs(args).getInt("port")
    }catch{
      case exception: Exception=> {
        exception.printStackTrace()
        port = 6666
      }
    }

    //初始化环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(1);

    //获取数据
    val data = env.socketTextStream("david",port)

    import org.apache.flink.api.scala._

    //获取数据并进行切分，生成一个个单词
    val words: DataStream[String] = data.flatMap(line => line.split(" "))
    //将一个个单词，生成一个个对偶元组
    val tups = words.map(w => WordwithCount(w, 1))
    //分组
    val grouped = tups.keyBy("word")

    //设置窗口时间长度，及滑动间隔
    val window = grouped.timeWindow(Time.seconds(2), Time.seconds(1))
    //聚合
    //第一种写法
    //val results = window.sum("count")
    //第二种写法
    val results = window.reduce((a,b) => WordwithCount(a.word,a.count+b.count))
    //打印
    results.print();
    //执行
    env.execute("scala wordCount")
  }

  case class WordwithCount(word: String, count: Int)

}
