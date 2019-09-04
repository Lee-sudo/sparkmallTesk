package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.AdsLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DateAreaCityAdCountHandler {



  //创建一个时间转换类对象
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")
  /**
    *取每天每个大区各城市广告点击总数
    * @param filterAdsLogDStream  过滤后的数据集
    */
  def getDateAreaCityAdCount(filterAdsLogDStream: DStream[AdsLog]) = {

    //1.转换数据格式
    val dateAreaCityAdToOne: DStream[((String, String, String, String), Long)] = filterAdsLogDStream.map(adsLong => {
      //获取年月日
      val date: String = sdf.format(new Date(adsLong.timestamp))

      ((date, adsLong.area, adsLong.city, adsLong.adid), 1L)
    })

    //2.有状态的统计总数
    val dateAreaCityAdToCount: DStream[((String, String, String, String), Long)] = dateAreaCityAdToOne.updateStateByKey((seq: Seq[Long], status: Option[Long]) => {
      //统计当前批次的总数
      val sum: Long = seq.sum

      //将当前批次与之前状态中的数据结合
      Some(sum + status.getOrElse(0L))
    })

    //3.返回
    dateAreaCityAdToCount

  }

  /**
    * 将每天每个大区各城市广告点击总数存入Redis
    * @param dateAreaCityAdToCount
    */
  def saveDateAreaCityAdCountToRedis(dateAreaCityAdToCount: DStream[((String, String, String, String), Long)]) = {
    dateAreaCityAdToCount.foreachRDD(rdd =>{

      rdd.foreachPartition(items =>{

        //获取redis连接
        val jedis: Jedis = RedisUtil.getJedisClient

        //方案一：处理数据（写入redis
//        items.foreach{case((date,area,city,adid),count) =>
//            val redisKey = s"date_area_city_ad_$date"
//            val hashKey = s"$area:$city:$adid"
//            jedis.hset(redisKey,hashKey,count.toString)
//        }

        //方案二：转换数据结构，将数据转换为保持在Redis的形式
        val dateToAreaCityAdCountMap: Map[String, Map[String, String]] = items.toMap.groupBy(_._1._1).map { case (date, item) =>
          val stringToLong: Map[String, String] = item.map { case ((_, area, city, ad), count) =>
            (s"$area:$city:$ad", count.toString)
          }
          (date, stringToLong)
        }

        //方案二：将数据批量添加至Redis
        dateToAreaCityAdCountMap.foreach{case (date,map) =>
           val redisKey = s"date_area_city_ad_$date"

          //导入隐式转换
          import  scala.collection.JavaConversions._

          jedis.hmset(redisKey,map)
        }

        //关闭
        jedis.close()

      })

    })

  }

}
