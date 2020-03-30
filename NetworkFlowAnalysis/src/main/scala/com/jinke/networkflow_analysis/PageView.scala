package com.jinke.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName com.jinke.networkflow_analysis
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/25 - 11:23
  */
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
object PageView {
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
      .filter(_.behavior=="pv")//只统计pc操作
      .map(data => ("pv",1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    dataStrean.print("pv count")
    fsEnv.execute("PageView")
  }
}
