package com.atguigu.practice.sintest

import com.atguigu.practice.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * @ Author: yanchen
 * @ Date: 2020/9/14 8:45
 * @ Desc:
 */
object FileSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputPath = "D:\\BigData0317\\26_Flink\\flink-scala\\src\\input\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)


    //TODO 1.先转换成样例类类型
    val dataStream = inputStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)//包装成SensorReading返回
      })
  }
}
