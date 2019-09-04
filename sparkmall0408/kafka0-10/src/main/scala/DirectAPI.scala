import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object DirectAPI {

  def main(args: Array[String]): Unit = {

    //1.创建配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DirectAPI")

    //2.创建SparkContext对象
    val sc = new SparkContext(conf)

    //3.获取ssc
    val ssc = new StreamingContext(sc,Seconds(3))

    //ck
    ssc.checkpoint("./ck")

    //封装参数
    val kafkaPara = Map(
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      "group.id" -> "bigdata0409",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val topics = Map("test" -> 1)

    //5.基于Direct方式消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array("test"), kafkaPara))

    //6.打印数据
    kafkaDStream.map(recode => recode.value()).print()

    //启动
    ssc.start()
    ssc.awaitTermination()



  }

}
