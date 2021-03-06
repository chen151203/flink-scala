package com.atguigu.practice
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.collection.immutable
import scala.util.Random

/**
 * @ Author: yanchen
 * @ Date: 2020/9/11 19:29
 * @ Desc:
 */
//定义样例类,温度传感器
case class SensorReading(id:String,timestamp:Long,temperature:Double)
object SourceTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //1.从集合中读取
    val dataList=List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    )
    val stream1=env.fromCollection(dataList)
    //2.从文件中读取数据
    val inputPath = "D:\\BigData0317\\26_Flink\\flink-scala\\src\\input\\sensor.txt"
    val stream2 = env.readTextFile(inputPath)

    //3.从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop102:9092")
    properties.setProperty("group.id","consumer-group")
    val stream3 = env.addSource(new FlinkKafkaConsumer[String]("sensor",new SimpleStringSchema(),properties))
    //stream3.print()

    //4.自定义source
    val stream4=env.addSource(new MySensorSource())
    stream4.print()
    env.execute()

  }

  class MySensorSource() extends SourceFunction[SensorReading]{

    //定义一个标识位flag,用来表示数据源是否正常运行发出数据
    var running:Boolean = true
    override def cancel(): Unit = running=false

    override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
      //定义一个随机数生成器
      val rand = new Random()

      //随机生成一组(10个)传感器的初始温度:(id temp)
      var curTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(i => ("sensor" + i, rand.nextDouble() * 100))


      //定义无限循环,不停的产生数据,除非被取消
      while (running){
        // 在上次数据基础上微调,更新温度
        curTemp=curTemp.map(
          data => (data._1,data._2 + rand.nextGaussian())
        )
        //获取当前时间戳,加入到数据中,调用ctx.collect发出数据
        val curTime = System.currentTimeMillis()
        curTemp.foreach(data => ctx.collect(SensorReading (data._1,curTime,data._2)))

        Thread.sleep(100)
      }

    }
  }

}
