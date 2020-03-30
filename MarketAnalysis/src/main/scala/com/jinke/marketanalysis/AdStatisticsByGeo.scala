package com.jinke.marketanalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
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
  * @date 2020/3/29 - 19:56
  */
//输入的广告点击事件样例类
case class AdClickEvent(UserId:Long,adId:Long,province:String,city:String,timestamp:Long)
//按照省分统计的输出结果样例类
case class CountByProvince(windowEnd:String,province:String,count:Long)
//黑名单过滤
case class BlackListWarning(userId:Long,adId:Long,msg:String)
object AdStatisticsByGeo {
  //定义侧输出流的Tag
  val blackListOutputTag :OutputTag[BlackListWarning]=new OutputTag[BlackListWarning]("blackList")
  def main(args: Array[String]): Unit = {
val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    fsEnv.setParallelism(1)

    val Resource = getClass.getResource("/AdClickLog.csv")
    val dataStream = fsEnv.readTextFile(Resource.getPath)
    .map(data => {
      val dataArray = data.split(",")
      AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
    }).assignAscendingTimestamps(_.timestamp*1000L)

      val filterBlackListStream = dataStream
          .keyBy(data =>(data.UserId,data.adId))
        .process(new filterBlackListUser(100))

    val adCountStream = filterBlackListStream.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    adCountStream.print()
    filterBlackListStream.getSideOutput(blackListOutputTag).print("blackList")
    fsEnv.execute("AdStatisticsByGeo")
  }
  class filterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long,Long),AdClickEvent,AdClickEvent]{
    //保存当前用户对当前广告的点击量
    lazy val countState:ValueState[Long]=getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state",classOf[Long]))
    //标记当前(用户，广告)作为key是否第一次发送到黑名单
    lazy val firstState:ValueState[Boolean]=getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("first-state",classOf[Boolean]))
    //保存定时触发的时间戳，届时清空重置状态
    lazy val resettime:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-time",classOf[Long]))
    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      val curCount = countState.value()
      if(curCount==0){
        val ts = (ctx.timerService().currentProcessingTime()/(24*60*60*1000)+1)*(24*60*60*1000)
        resettime.update(ts)
        ctx.timerService().registerEventTimeTimer(ts)
      }
      //如果计数已经超过上限，则加入黑名单，用则输出流输出报警信息
      if(curCount>=maxCount){
        if(!firstState.value()){
          firstState.update(true)
          ctx.output(blackListOutputTag,BlackListWarning(value.UserId,value.adId,"Click Over "+maxCount+" times today."))
        }
        return
      }
      //点击计数加1
      countState.update(curCount+1)
      out.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      if(timestamp==resettime.value()){
        firstState.clear()
        countState.clear()
        resettime.clear()
      }
    }
  }
}
class AdCountAgg() extends AggregateFunction[AdClickEvent,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickEvent, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}
class AdCountResult() extends WindowFunction[Long,CountByProvince,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(new Timestamp(window.getEnd).toString,key,input.iterator.next()))
  }
}
