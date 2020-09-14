package com.atguigu.day2

import org.apache.flink.streaming.api.scala._

/**
 * @ Author: yanchen
 * @ Date: 2020/9/11 14:01
 * @ Desc:
 */
object KeyedStreamExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .filter(r=>r.id.equals("sensor_1"))
    val keyedStream: KeyedStream[SensorReading, String] = stream.keyBy(r => r.id)

    val maxStream: DataStream[SensorReading] = keyedStream.max(2)
    maxStream.print()

    env.execute()
  }
}
