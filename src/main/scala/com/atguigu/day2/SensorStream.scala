package com.atguigu.day2

import org.apache.flink.streaming.api.scala._

/**
 * @ Author: yanchen
 * @ Date: 2020/9/11 9:40
 * @ Desc:
 */
object SensorStream {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream=env.addSource(new SensorSource)
    stream.print()
    env.execute()
  }
}
