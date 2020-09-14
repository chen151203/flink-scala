package com.atguigu.practice
import org.apache.flink.api.common.functions.{FilterFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala._
/**
 * @ Author: yanchen
 * @ Date: 2020/9/12 9:28
 * @ Desc:
 */
object TransformTest {
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

        //TODO 2分组聚合,输出每个传感器当前最小值
        val aggStream = dataStream
            .keyBy("id")//根据id进行分组
            .minBy("temperature")//和min不用的是,这个minBY会把所有数据都输出
        //TODO 3.需要输出当前的最小温度,以及最近的时间戳,需要用reduce
        val resultStream = dataStream
            .keyBy("id")
//            .reduce((curState,newData)=>
//            SensorReading(curState.id,newData.timestamp,curState.temperature.min(newData.temperature))
//            )
            .reduce(new MyReduceFunction)

        //TODO 4.多流转换操作
        // 4.1 分流,将传感器温度分成低温和高温
        val splitStream = dataStream
            .split(data => {
                if (data.temperature > 30) Seq("High") else Seq ("Low")
            })
        val highTempStream = splitStream.select("High")
        val lowTempStream = splitStream.select("Low")
        val allTempStream = splitStream.select("High","Low")
//        highTempStream.print("high")
//        lowTempStream.print("low")
//        allTempStream.print("all")

        //TODO
        // 4.2 合流 connect
        val warningStream = highTempStream.map(data =>(data.id,data.temperature) )//高温报警,转化成报警的信息
        val connectedStreams = warningStream.connect(lowTempStream)

        //用coMap对数据进行分别处理
        val coMapResultStream = connectedStreams
            .map(
                waringData=>(waringData._1,waringData._2,"warning"),
                lowTempData =>(lowTempData.id,"healthy")
            )

        //TODO
        // 4.3 union 合流  可以多条流合并
        val unionStream = highTempStream.union(lowTempStream)

        //dataStream.print()

        env.execute()

    }


}
class MyReduceFunction extends ReduceFunction[SensorReading]{
    override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {

        SensorReading(t.id,t1.timestamp,t.temperature.min(t1.temperature))
    }
}
class MyFilter extends FilterFunction[SensorReading]{//要处理的事件类型是SensorReading
    override def filter(value: SensorReading): Boolean = {

        value.id.startsWith("sensor_1")
    }
}