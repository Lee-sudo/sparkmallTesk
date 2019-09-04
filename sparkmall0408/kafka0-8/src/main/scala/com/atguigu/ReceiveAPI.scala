package com.atguigu

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ReceiveAPI {

  def main(args: Array[String]): Unit = {
    //1.获取sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReceiveAPI")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.获取SSC
    val ssc = new StreamingContext(sc,Seconds(3))
    //4.封装参数
    val kafkaPara = Map(
      "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop103:2181",
      "group.id" -> "bigdata0408"
    )
    val topics = Map("test" -> 1)

    //5.基于Receiver方式消费kafkas数据
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPara, topics, StorageLevel.MEMORY_ONLY)


    //6.打印数据
    kafkaDStream.map(_._2).print()

    //7.启动
    ssc.start()
    ssc.awaitTermination()

  }

}
