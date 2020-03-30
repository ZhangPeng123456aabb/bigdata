package com.jinke.marketanalysis

/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName com.jinke.marketanalysis
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/25 - 16:42
  */
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random
//输入数据样例类
case class MarketingUserBehavior(userId:String,behavior:String,channel:String,timestamp:Long)
//输出结果样例类
case class MarketingViewCount(windowStart:String,windowEnd:String,channel:String,behavior:String,count:Int)
object AppMarketingChannel {
  def main(args: Array[String]): Unit = {
  val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    fsEnv.setParallelism(1)
    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream = fsEnv.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior !="UNINSTALL")
      .map(data => {
        ((data.channel,data.behavior),1L)
      })
      .keyBy(_._1)//以渠道和行为类型作为key分组
      .timeWindow(Time.hours(1),Time.seconds(10))
      .process(new MarketingCountByChannel())

    dataStream.print()
    fsEnv.execute("AppMarketingChannel")
  }
}
//自定义数据源
class SimulatedEventSource() extends SourceFunction[MarketingUserBehavior]{
  var isRunning = true
  //定义用户行为的集合
  val behaviorTypes:Seq[String]=Seq("CLICK","DOWNLOAD","INSTALL","UNINSTALL")
  //定义渠道集合
  val channelSets:Seq[String]=Seq("wechat","weibo","appstore","huaweistore")
  //定义一个随机数发生器
  val rand:Random=new Random()
  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    //定义一个生成数据的上限
    val maxElement = Long.MaxValue
    var count = 0L
    while (isRunning && count < maxElement) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSets(rand.nextInt(channelSets.size))
      val ts = System.currentTimeMillis()
      ctx.collect(MarketingUserBehavior(id, behavior, channel, ts))
      count += 1
      TimeUnit.MILLISECONDS.sleep(10L)
    }
  }
  override def cancel(): Unit = isRunning=false
}
//自定义处理函数
class MarketingCountByChannel() extends ProcessWindowFunction[((String,String),Long),MarketingViewCount,(String,String),TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(context.window.getStart).toString
    val endTs = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behaviorType = key._2
    val count = elements.size
    out.collect(MarketingViewCount(startTs,endTs,channel,behaviorType,count))
  }
}
