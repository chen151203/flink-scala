package com.atguigu.day2

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

/**
 * @ Author: yanchen
 * @ Date: 2020/9/11 11:38
 * @ Desc:
 */
object FilterFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream.filter(r => r.temperature>0.0)
    stream.filter(new MyFilterFunction)
    stream.filter(new FilterFunction[SensorReading] {
      override def filter(t: SensorReading) = t.temperature>0.0
    }).print()
    env.execute()
  }
  class MyFilterFunction extends FilterFunction[SensorReading]{
    override def filter(t: SensorReading): Boolean = t.temperature>0.0
  }
}
