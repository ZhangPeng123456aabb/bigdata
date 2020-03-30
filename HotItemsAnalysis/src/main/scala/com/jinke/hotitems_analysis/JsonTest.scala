package com.jinke.hotitems_analysis


import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import redis.clients.jedis.Jedis

/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName com.jinke.hotitems_analysis
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/25 - 19:22
  */
object JsonTest {
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
    val jedis = new Jedis("Spark",6379)
    dataStream.flatMap(data => {

      jedis.set("count",data);
    })
    dataStream.print()
    fsEnv.execute("JsonTest")
  }
}
