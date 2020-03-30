package com.ZhongJian.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName com.ZhongJian.orderpay_detect
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/30 - 21:15
  */
//定义接收流事件冲突
case class ReceiptEvent(txId:String,payChannel:String,eventTime:Long)
object TxMacthDetect {
  //定义侧输出流tag
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")
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
    //将两条流连接起来，共同处理
    val processedStream = orderEventStream.connect(receiptEventStream)
      .process(new TxPayMatch())
    processedStream.print("matched")
    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")
    fsEnv.execute("TxMacthDetect")
  }
  class TxPayMatch extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
    //定义状态来保存已经到达的订单支付事件和到账事件
    lazy val payState:ValueState[OrderEvent]=getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state",classOf[OrderEvent]))
    lazy val receiptState:ValueState[ReceiptEvent]=getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state",classOf[ReceiptEvent]))
    //订单支付事件的处理
    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //判断有没有对应的到账事件
    val receipt = receiptState.value()
    if(receipt != null){
    //如果已经有receipt,在主流输出匹配信息，清空状态
      out.collect((pay,receipt))
      payState.clear()
    }else{
      //如果还没到，那么把pay存入状态,并且注册一个定时器等待
      payState.update(pay)
      ctx.timerService().registerEventTimeTimer(pay.eventTime*1000L+5000L)
    }
    }
   //到账事件的处理
    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //同样的处理流程
      val pay = payState.value()
      if(pay != null){
        out.collect((pay,receipt))
        payState.clear()
      }else{
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer(receipt.eventTime*1000L+5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //到时间了，如果还没有收到某个事件，那么输出报警信息
      if(payState.value() != null){
        //receipt还没有到来，输出pay到侧输出流
        ctx.output(unmatchedPays,payState.value())
      }
      if(receiptState.value() != null){
        ctx.output(unmatchedReceipts,receiptState.value())
      }
      payState.clear()
      receiptState.clear()
    }
  }
}
