package com.ZhongJian.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName com.ZhongJian.orderpay_detect
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/30 - 16:05
  */
object OrderTimeoutWithoutCep {
  val orderTimeoutOutputTag=new OutputTag[OrderResult]("orderTimeout")
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
  //定义process function进行超时监测
 //   val timeoutWarningStream = orderEventStream.process(new OrderTimeoutWarning())
    val orderResultStream = orderEventStream.process(new OrderPayMatch())
    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")
    fsEnv.execute("OrderTimeoutWithoutCep")
  }
  class OrderPayMatch extends KeyedProcessFunction[Long,OrderEvent,OrderResult]{
    //保存pay状态是否来过
    lazy val isPayState:ValueState[Boolean]=getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed-state",classOf[Boolean]))
    //保存定时器的时间戳为状态
    lazy val TimerState:ValueState[Long]=getRuntimeContext.getState(new ValueStateDescriptor[Long]("Timer-state",classOf[Long]))
    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    //先读取状态
      val isPayed = isPayState.value()
      val timerTs = TimerState.value()
      //根据事件的类型进分类判断
      if(value.eventType=="create"){
        //1.如果create事件，接下来判断pay是否来过
        if(isPayed){
        //1.1如果已经pay过，匹配成功，输出主流，清空状态
          out.collect(OrderResult(value.orderId,"payed successfully"))
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayState.clear()
          TimerState.clear()
        }else{
          //1.2如果没有payed过，注册定时器等待pay的到来
          val ts = value.eventTime*1000L+15*60*1000L
          ctx.timerService().registerEventTimeTimer(ts)
          TimerState.update(ts)
        }
      }else if(value.eventType=="pay"){
          //2.如果是pay事件，那么判断是否create过，用timer表示
        if(timerTs>0){
          //2.1如果有定时器，说明有create来过
          //继续判断是否超过timeout时间
          if(timerTs>value.eventTime*1000L){
          //2.1.1如果定时器时间还没到，则输出成功匹配
            out.collect(OrderResult(value.orderId,"payed successfully"))
          }else{
            //2.1.2如果当前时间还没到，那么输出到侧输出流
            ctx.output(orderTimeoutOutputTag,OrderResult(value.orderId,"payed but order timeout"))
          }
          //输出结束，清空状态
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayState.clear()
          TimerState.clear()
        }else{
          //2.2 pay先到,更新状态，注册定时器，等待create
          isPayState.update(true)
          ctx.timerService().registerEventTimeTimer(value.eventTime*1000L)
          TimerState.update(value.eventTime*1000L)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      //根据状态的值，判断哪个数据没来
      if(isPayState.value()){
        //如果为true,表示pay先到，没有等到create
        ctx.output(orderTimeoutOutputTag,OrderResult(ctx.getCurrentKey,"payed but not found create"))
      }else{
        //否则，表示create到来,没有等到pay
        ctx.output(orderTimeoutOutputTag,OrderResult(ctx.getCurrentKey,"created but not found pay"))
      }
      isPayState.clear()
      TimerState.clear()
    }
  }
}
//自定义处理函数
class OrderTimeoutWarning extends KeyedProcessFunction[Long,OrderEvent,OrderResult]{
  //保存pay状态是否来过
  lazy val isPayState:ValueState[Boolean]=getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed-state",classOf[Boolean]))
  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
  //先取出状态标识位
    val isPayed = isPayState.value()
    if(value.eventType=="create" && !isPayed){
      //如果遇到了create事件，并且pay没有来过，注册定时器等待
      ctx.timerService().registerEventTimeTimer(value.eventTime*1000L+15*60*1000L)
    }else if(value.eventType=="pay"){
      //如果是pay事件，直接把状态改为true
      isPayState.update(true)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    //判断isPayed是否为true
    val isPayed = isPayState.value()
    if(isPayed){
      out.collect(OrderResult(ctx.getCurrentKey,"order payed successfully"))
    }else{
      out.collect(OrderResult(ctx.getCurrentKey,"order payed fail"))
    }
    //清空状态
    isPayState.clear()
  }
}
