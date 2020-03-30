package com.ZhongJian.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName com.ZhongJian.orderpay_detect
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/30 - 23:06
  */
object TxMatchByJoin {
  def main(args: Array[String]): Unit = {
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    fsEnv.setParallelism(1)
    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //订单事件流
    //val resource = getClass.getResource("/OrderLog.csv")
    //val orderEventStream = fsEnv.readTextFile(resource.getPath)
    val orderEventStream = fsEnv.socketTextStream("Spark", 7777)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong,dataArray(1).trim,dataArray(2).trim,dataArray(3).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime*1000L)
      .keyBy(_.txId)
    //支付到账事件流
    //val receiptResource =  getClass.getResource("/ReceiptLog.csv")
    //val receiptEventStream = fsEnv.readTextFile(receiptResource.getPath)
    val receiptEventStream = fsEnv.socketTextStream("Spark", 8888)
      .map(data =>{
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0).trim,dataArray(1).trim,dataArray(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime*1000L)
      .keyBy(_.txId)
    //join处理
    val processedStream = orderEventStream.intervalJoin(receiptEventStream)
      .between(Time.seconds(-5),Time.seconds(5))
      .process(new TxPayMatchByJoin())
    processedStream.print()
    fsEnv.execute("TxMatchByJoin")
  }
}
class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left,right))
  }
}
