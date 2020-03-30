package com.jinke.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName com.jinke.networkflow_analysis
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/25 - 0:29
  */
//输入数据的样例类
case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)
//窗口聚合结果的样例类
case class UrlViewCount(url:String,windowEnd:Long,count:Long)
object NetworkFlow {
  def main(args: Array[String]): Unit = {
  val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    fsEnv.setParallelism(1)
    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream = fsEnv.readTextFile("F:\\study\\IDEA_project\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
      .map(data =>{
        val dataArray=data.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3).trim).getTime
        ApacheLogEvent(dataArray(0).trim,dataArray(1).trim,timestamp,dataArray(5).trim,dataArray(6).trim)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.seconds(5))
      .allowedLateness(Time.seconds(60))
      .aggregate(new CountAgg(),new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    dataStream.print()
    fsEnv.execute("NetworkFlow")
  }
}
//自定义预聚合函数
class CountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}
//自定义窗口处理函数
class WindowResult() extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
  out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
  }
}
//自定义排序输出处理函数
class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{
  lazy val urlState:ListState[UrlViewCount]=getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state",classOf[UrlViewCount]))
  override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
  urlState.add(i)
    context.timerService().registerEventTimeTimer(i.windowEnd+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //首先从状态中拿到数据
    val allUrlViews:ListBuffer[UrlViewCount]=new ListBuffer[UrlViewCount]()
    val iter = urlState.get().iterator()
    while(iter.hasNext){
      allUrlViews += iter.next()
    }
    urlState.clear()
    val sortedUrlViews = allUrlViews.sortWith(_.count > _.count).take(topSize)
    //格式化结果输出
    val result = new StringBuilder()
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    for(i <- sortedUrlViews.indices){
      val currentUrlView = sortedUrlViews(i)
      result.append("NO").append(i+1).append(":")
        .append(" URL=").append(currentUrlView.url)
        .append(" 访问量=").append(currentUrlView.count).append("\n")
    }
    result.append("===============================")
    out.collect(result.toString())
  }
}
