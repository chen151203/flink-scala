package com.atguigu.practice


import org.apache.flink.api.scala._
/**
 * @ Author: yanchen
 * @ Date: 2020/9/10 14:00
 * @ Desc:
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "D:\\BigData0317\\26_Flink\\flink-scala\\src\\input\\hello"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    val result: DataSet[(String, Int)] = inputDataSet
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    result.print()
  }
}
