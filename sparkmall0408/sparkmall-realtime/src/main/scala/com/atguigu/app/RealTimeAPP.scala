package com.atguigu.app

import com.atguigu.bean.AdsLog
import com.atguigu.handler._
import com.atguigu.utils.MyKafkaUtil
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.SparkConf

object RealTimeAPP {

  def main(args: Array[String]): Unit = {

    //1.获取sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealTimeAPP")

    //2.创建SparkContext
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //设置检查点
    ssc.sparkContext.setCheckpointDir("./dateAreaCityAdCountCK")

    //3.读取kafka数据
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc,"ads_log")

    //4.将数据集内容转换为样例类对象
    val adsLogDStream: DStream[AdsLog] = kafkaDStream.map { case (key, value) =>
      val splits: Array[String] = value.split(" ")
      AdsLog(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))

    }

    //需求五：根据黑名单过滤数据集
    val filterAdsLogDStream: DStream[AdsLog] = BlackListHandler.filterDataByBlackList(ssc.sparkContext, adsLogDStream)

    //复用
    filterAdsLogDStream.cache()

    //需求六：获取每天每个大区各城市广告点击总数
    val dateAreaCityAdToCount: DStream[((String, String, String, String), Long)] = DateAreaCityAdCountHandler.getDateAreaCityAdCount(filterAdsLogDStream)

    //需求八：最近一小时广告点击量
    LastHourAdClickCountHandler.saveLastHourAdClickCountToRedis(filterAdsLogDStream)

    dateAreaCityAdToCount.cache()

    //需求六：将每天每个大区各城市广告点击总数存入Redis
    DateAreaCityAdCountHandler.saveDateAreaCityAdCountToRedis(dateAreaCityAdToCount)

    ////需求七：每天各地区 top3 热门广告并将数据保存至Redis
    Top3DateAreaAdCountHandler.saveDateAreaAdCountToRedis(dateAreaCityAdToCount)


    //需求五：校验点击次数，若超过100则加入黑名单
    BlackListHandler.checkDataToBlickList(filterAdsLogDStream)


    //启动
    ssc.start()
    ssc.awaitTermination()



  }

}
