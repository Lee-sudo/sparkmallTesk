package com.atguigu

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

object DirectAPHandler {

  def main(args: Array[String]): Unit = {
    //错误写法
    def main(args: Array[String]): Unit = {

      //1.获取sparkConf
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReceiveAPI")

      //2.创建SparkContext
      val sc = new SparkContext(sparkConf)

      //3.获取SSC
      val ssc = new StreamingContext(sc, Seconds(3))

      ssc.checkpoint("./ck")

      //4.封装参数
      val kafkaPara = Map(
        "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
        "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop103:2181",
        "group.id" -> "bigdata0409"
      )
      val topics = Map("test" -> 1)

      val partitionToLong: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
      val topicAnd = TopicAndPartition("aa", 1)
      partitionToLong + (topicAnd -> 1000L)

      var offsetRanges = Array.empty[OffsetRange]

      //5.基于Direct方式消费kafka数据
      //val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String,StringDecoder, StringDecoder](ssc, kafkaPara, Set("test"))

      def messageHandler(m: MessageAndMetadata[String, String]): String = {
        m.message()
      }

      val kafkaDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
        ssc,
        kafkaPara,
        partitionToLong,
        messageHandler)

      val kafkaDStream2: DStream[String] = kafkaDStream.transform(rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        rdd
      })

      kafkaDStream2.foreachRDD(rdd =>{
        for (elem <- offsetRanges) {
          val offset: Long = elem.untilOffset
          val topic: String = elem.topic
          val partition: Int = elem.partition
        }
      })

      kafkaDStream.print()



      //6.打印数据
      //kafkaDStream.map(_._2).print()

      //7.启动
      ssc.start()
      ssc.awaitTermination()

    }
  }

}
