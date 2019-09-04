package com.atguigu

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object DirectAPI {
  //错误写法
  def main(args: Array[String]): Unit = {

    //1.获取sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReceiveAPI")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.获取SSC
    val ssc = new StreamingContext(sc,Seconds(3))

    ssc.checkpoint("./ck")

    //4.封装参数
    val kafkaPara = Map(
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop103:2181",
      "group.id" -> "bigdata0409"
    )
    val topics = Map("test" -> 1)

    //5.基于Direct方式消费kafka数据
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPara, Set("test"))


    //6.打印数据
    kafkaDStream.map(_._2).print()

    //7.启动
    ssc.start()
    ssc.awaitTermination()

  }

}
