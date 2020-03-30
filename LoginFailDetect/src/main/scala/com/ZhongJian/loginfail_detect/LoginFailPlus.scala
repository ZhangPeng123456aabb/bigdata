package com.ZhongJian.loginfail_detect
import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName com.ZhongJian.loginfail_detect
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/30 - 10:40
  */
//输入的登录事件样例类
case class LoginEvent(userId:Long,ip:String,eventType:String,eventTime:Long)
//输出的异常报警信息
case class Warning(userId:Long,firstFailTime:Long,lastFailTime:Long,warningMsg:String)
object LoginFailPlus {
  def main(args: Array[String]): Unit = {
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    fsEnv.setParallelism(1)
    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //读取事件数据
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = fsEnv.readTextFile(resource.getPath)
      .map(data =>{
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong,dataArray(1).trim,dataArray(2),dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime*1000L
      })
    val warningStream = loginEventStream
      .keyBy(_.userId) //以用户id进行分组
      .process(new LoginWarning (2))

    warningStream.print()
    fsEnv.execute("LoginFail plus")
  }
}
class LoginWarning(maxFailTimes:Int) extends KeyedProcessFunction[Long,LoginEvent,Warning]{
  //定义状态，保存2秒内的所有登录失败事件
  lazy val loginFailState:ListState[LoginEvent]=getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state",classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 首先按照 type 做筛选，如果 success 直接清空，如果 fail 再做处理
    if ( value.eventType == "fail" ){
      // 如果已经有登录失败的数据，那么就判断是否在两秒内
      val iter = loginFailState.get().iterator()
      if ( iter.hasNext ){
        val firstFail = iter.next()
        // 如果两次登录失败时间间隔小于 2 秒，输出报警
        if ( value.eventTime < firstFail.eventTime + 2 ){
          out.collect( Warning( value.userId, firstFail.eventTime, value.eventTime,
            "login fail in 2 seconds." ) )
        }
        // 把最近一次的登录失败数据，更新写入 state 中
        val failList = new util.ArrayList[LoginEvent]()
        failList.add(value)
        loginFailState.update( failList )
      } else {
        // 如果 state 中没有登录失败的数据，那就直接添加进去
        loginFailState.add(value)
      }
    } else
      loginFailState.clear()
  }
}
