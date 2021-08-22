package com.aiguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val paramTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = paramTool.get("host")
    val port: Int = paramTool.getInt("port")

    // 创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(4)

    // 接收 socket 文本流

    val inputDataStream = env.socketTextStream(host, port)

    val resultDataStream = inputDataStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    resultDataStream.print().setParallelism(1)

    // 启动任务执行
    env.execute("Stream word count")

  }

}
