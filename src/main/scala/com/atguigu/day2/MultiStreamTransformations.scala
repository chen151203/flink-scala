package com.atguigu.day2


import org.apache.flink.streaming.api.scala._
/**
 * @ Author: yanchen
 * @ Date: 2020/9/11 16:47
 * @ Desc:
 */
object MultiStreamTransformations {

val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)



env.execute()


}
