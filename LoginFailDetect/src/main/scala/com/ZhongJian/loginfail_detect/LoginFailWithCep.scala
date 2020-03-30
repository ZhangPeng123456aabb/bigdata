package com.ZhongJian.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName com.ZhongJian.loginfail_detect
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/30 - 13:24
  */
object LoginFailWithCep {
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
      .keyBy(_.userId) //以用户id进行分组
    //2.定义模式匹配
    val loginFailPattern=Pattern.begin[LoginEvent]("begin").where(_.eventType=="fail")
      .next("next").where(_.eventType=="fail")
      .within(Time.seconds(2))
    //3.在事件流上应用模式，得到一个pattern stream
    val patternStream = CEP.pattern(loginEventStream,loginFailPattern)
    //从pattern Stream上应用select function,检测出匹配事件序列
    val loginFailDataStream = patternStream.select(new LoginFailMatch())
    loginFailDataStream.print()
    fsEnv.execute("login fail with cep job")
  }
}
class LoginFailMatch() extends PatternSelectFunction[LoginEvent,Warning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    //从map中按照名称取出对应的事件
    val firstFail = map.get("begin").iterator().next()
    val lastFail = map.get("next").iterator().next()
    Warning(firstFail.userId,firstFail.eventTime,lastFail.eventTime,"login fail")
  }
}
