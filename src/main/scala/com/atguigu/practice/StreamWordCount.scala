package com.atguigu.practice
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @ Author: yanchen
 * @ Date: 2020/9/10 14:18
 * @ Desc:
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //从外部命令中提取参数,作为socket主机名和端口号
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = parameterTool.get("host")
    val port: Int = parameterTool.getInt("port")

    val inputDataStream: DataStream[String] = env.socketTextStream("host", port)
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    resultDataStream.print()
    //启动任务执行
    env.execute("Stream word count")

  }
}
