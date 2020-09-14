package com.atguigu.day2

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

/**
 * @ Author: yanchen
 * @ Date: 2020/9/11 14:11
 * @ Desc:
 */
object KeyedStreamReduceExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream
      .map(r => (r.id,r.temperature))
      .keyBy(r => r._1)
      .reduce((r1,r2)=>(r1._1,r1._2.max(r2._2)))
      //.print()

    stream
      .map(r=>(r.id,r.temperature))
      .keyBy(r=>r._1)
      .reduce(new MyReduceFunction)
      .print()
    env.execute()
  }

}

case class MyReduceFunction() extends ReduceFunction[(String,Double)] {
  override def reduce(value1: (String, Double), valuse2: (String, Double)): (String, Double) = {
    (value1._1,valuse2._2.max(valuse2._2))
  }
}
