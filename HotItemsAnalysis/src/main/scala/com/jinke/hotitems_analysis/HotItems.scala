package com.jinke.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName com.jinke.hotitems_analysis
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/24 - 15:55
  */
//定义输入数据的样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
//定义窗口聚合结果样例类
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)
object HotItems {
  def main(args: Array[String]): Unit = {
     //1.创建环境
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    fsEnv.setParallelism(1)
    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //2.读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "Spark:9092")
    properties.setProperty("group.id", "g1")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val dataStream = fsEnv.addSource(new FlinkKafkaConsumer[String]("sensor",new SimpleStringSchema(),properties))
    //val dataStream = fsEnv.readTextFile("F:\\study\\IDEA_project\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp*1000L)
    //3.transform处理数据
    val processStream = dataStream
        .filter(_.behavior=="pv")
        .keyBy(_.itemId)
        .timeWindow(Time.hours(1),Time.minutes(5))
        .aggregate(new CountAgg(),new WindowResult()) //窗口聚合
        .keyBy(_.windowEnd)  //按照窗口分组
        .process(new TopNHotItems(3))
    //Sink控制台打印输出
    processStream.print("dataStream")

    //4.开始执行
    fsEnv.execute("HotItems")
  }
}
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}
//自定义预聚合函数计算平均数
class AverageAgg() extends AggregateFunction[UserBehavior,(Long,Int),Double]{
  override def createAccumulator(): (Long, Int) = (0L,0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1+in.timestamp,acc._2+1)

  override def getResult(acc: (Long, Int)): Double = acc._1/acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1+acc1._1,acc._2+acc1._2)
}
//自定义窗口函数，输出ItemViewCount
class WindowResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
  }
}
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{
  private var itemState:ListState[ItemViewCount] = _
  override def open(parameters: Configuration): Unit = {
  itemState=getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state",classOf[ItemViewCount]))
  }
  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
  //把每条数据存入状态列表
    itemState.add(i)
    context.timerService()registerEventTimeTimer(i.windowEnd+1)
  }
  //定时器触发时，对所有数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //对所有state中的数据取出，放到一个list Buffer
    val allItems:ListBuffer[ItemViewCount]=new ListBuffer[ItemViewCount]
    for(item <- itemState.get()){
      allItems += item
    }
    //按照count大小排序
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    //清空状态，释放内存空间
    itemState.clear()
    //将排名结果格式化输出
    val result:StringBuilder = new StringBuilder
    result.append("时间，").append(new Timestamp(timestamp-1)).append("\n")
    //输出每一个商品的信息
    for(i <- sortedItems.indices){
      val currentItem = sortedItems(i)
      result.append("No").append(i+1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }
    result.append("====================================")
    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
