package com.jinke.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis
/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName com.jinke.networkflow_analysis
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/25 - 14:37
  */
object UvWithBloom {
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
      .map(data => ("dummyKey",data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())
    dataStrean.print()
    fsEnv.execute("uv with bloom job")
  }
}
class MyTrigger() extends Trigger[(String,Long),TimeWindow]{

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult =TriggerResult.CONTINUE

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}

  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult ={
    //每来一条数据，就直接触发窗口计算，并清空窗口状态
    TriggerResult.FIRE_AND_PURGE
  }
}
//定义布隆过滤器
class Bloom(size:Long) extends Serializable{
  //位图总的大小
  private val cap = if(size > 0) size else 1 << 27
  //定义哈希函数
  def hash(value:String,seed:Int):Long= {
    var result: Long = 0L
    for (i <- 0 until value.length) {
      for (i <- 0 until value.length) {
        result = result * seed + value.charAt(i)
      }
    }
    result & (cap - 1)
  }
}
class UvCountWithBloom() extends ProcessWindowFunction[(String,Long),UvCount,String,TimeWindow]{
  //定义redis连接
  lazy val jedis = new Jedis("Spark",6379)
  lazy val bloom = new Bloom(1 << 29)
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
  //位图的存储方式
    val storeKey = context.window.getEnd.toString
    var count = 0L
    //把每个窗口的uv count值也存入redis，存放内容为(windowEnd -> UvCount),所以先从redis中读取
    if(jedis.hget("count",storeKey) != null){
      count=jedis.hget("count",storeKey).toLong
    }
    //用布隆过滤器判断当前用户是否已经存在
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId,61)
    //定义一个标识位，判断redis为途中有没有这一位
    val isExist = jedis.getbit(storeKey,offset)
    if(!isExist){
      //如果不存在，位图对应位置1,count+1
      jedis.setbit(storeKey,offset,true)
      jedis.hset("count",storeKey,(count+1).toString)
      out.collect(UvCount(storeKey.toLong,count+1))
    }else{
      out.collect(UvCount(storeKey.toLong,count))
    }
  }
}
