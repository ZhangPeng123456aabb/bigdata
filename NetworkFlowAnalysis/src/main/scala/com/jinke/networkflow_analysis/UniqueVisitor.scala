package com.jinke.networkflow_analysis
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName com.jinke.networkflow_analysis
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/25 - 14:03
  */
case class UvCount(windowEnd:Long,uvCount:Long)
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    fsEnv.setParallelism(1)
    //用相对路径定义数据源
    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStrean = fsEnv.readTextFile(resource.getPath)
      .map(data =>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp*1000L)
      .filter(_.behavior=="pv")//只统计pv操作
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())
    dataStrean.print()
    fsEnv.execute("UniqueVisitor")
  }
}
class UvCountByWindow() extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //定义一个scala se,用于保存所有数据userId并去重
    var idSet = Set[Long]()
    //把当前窗口所有数据的ID收集到set中，最后输出set的大小
    for(userBehavior  <- input){
      idSet += userBehavior.userId
    }
    out.collect(UvCount(window.getEnd,idSet.size))
  }
}
