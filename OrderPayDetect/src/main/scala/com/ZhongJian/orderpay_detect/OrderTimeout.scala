package com.ZhongJian.orderpay_detect
import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName com.ZhongJian.orderpay_detect
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/30 - 14:28
  */
//定义输入订单事件的样例类
case class OrderEvent(orderId:Long,eventType:String,txId:String,eventTime:Long)
//定义输出结果的样例类
case class OrderResult(orderId:Long,resultMsg:String)
object OrderTimeout {
  def main(args: Array[String]): Unit = {
    //1.创建执行环境
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    fsEnv.setParallelism(1)
    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //2.读取数据源
    //val resource = getClass.getResource("/OrderLog.csv")
    //val orderEventStream = fsEnv.readTextFile(resource.getPath)
    val orderEventStream = fsEnv.socketTextStream("Spark", 5555)
    //3.转换数据
      .map(data =>{
      val dataArray = data.split(",")
      OrderEvent(dataArray(0).trim.toLong,dataArray(1).trim,dataArray(2).trim,dataArray(3).trim.toLong)
    })
        .assignAscendingTimestamps(_.eventTime*1000L)
        .keyBy(_.orderId)
    //4.定义一个匹配模式
    val orderPayPattern =Pattern.begin[OrderEvent]("begin").where(_.eventType=="create")
        .followedBy("follow").where(_.eventType=="pay")
        .within(Time.minutes(15))

    //5.把模式应用到Stream上，得到一个pattern stream
    val patternStream =CEP.pattern(orderEventStream,orderPayPattern)
    //6.调用select 方法，提取事件序列，，超时的事件做报警提示
     val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")
     val reaultStream = patternStream.select(orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())
    //7.打印输出
    reaultStream.print("payed")
    reaultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")
    //8.开启执行命令
    fsEnv.execute("OrderTimeout")
  }
}
//自定义超时事件序列处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent,OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId = map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId,"timeout")
  }
}
class OrderPaySelect extends PatternSelectFunction[OrderEvent,OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
  val payedOrderId = map.get("follow").iterator().next().orderId
    OrderResult(payedOrderId,"payed successfully")
  }
}
