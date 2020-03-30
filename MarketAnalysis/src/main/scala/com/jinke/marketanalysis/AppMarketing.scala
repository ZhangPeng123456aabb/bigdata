package com.jinke.marketanalysis

import java.lang
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName com.jinke.marketanalysis
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/28 - 23:38
  */
//输入数据样例类
case class MarketingUserBehavior(userId:String,behavior:String,channel:String,timestamp:Long)
//输出结果样例类
case class MarketingViewCount(windowStart:String,windowEnd:String,channel:String,behavior:String,count:Long)
object AppMarketing {
  def main(args: Array[String]): Unit = {
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    fsEnv.setParallelism(1)
    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream = fsEnv.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior !="UNINSTALL")
      .map( data => {
        ("dummyKey",1L)
      })
      .keyBy(_._1)//以渠道和行为类型作为key分组
      .timeWindow(Time.hours(1),Time.seconds(10))
      .aggregate(new CountAgg(),new MarketingCountTotal())

    dataStream.print()
    fsEnv.execute("app marketing job")
  }
}
class CountAgg() extends AggregateFunction[(String,Long),Long,Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Long), acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}
class MarketingCountTotal() extends WindowFunction[Long,MarketingViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(window.getStart).toString
    val endTs = new Timestamp(window.getEnd).toString
    val count = input.iterator.next()
    out.collect(MarketingViewCount(startTs,endTs,"app marketing","total",count))
  }
}
