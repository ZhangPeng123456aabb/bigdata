package com.jinke.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
/**
  * @ProjectName UserBehaviorAnalysis
  * @PackageName com.jinke.hotitems_analysis
  * @author ZhangPeng
  * @Email ZhangPeng1853093@126.com
  * @date 2020/3/24 - 23:49
  */
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("sensor")
  }
  def writeToKafka(topic: String):Unit={
     val props = new Properties()
    props.setProperty("bootstrap.servers", "Spark:9092")
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    //定义一个kafka producer
    val producer = new KafkaProducer[String,String](props)
    //从文件中读取数据，发送
    val bufferedSource = io.Source.fromFile("F:\\study\\IDEA_project\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for(line <- bufferedSource.getLines()){
      val record = new ProducerRecord[String,String](topic,line)
      producer.send(record)
    }
    producer.close()
  }
}
