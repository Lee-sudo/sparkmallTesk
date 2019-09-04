package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.AdsLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object LastHourAdClickCountHandler {

  //最近一个小时广告点击次数的RedisKey
  private val last_hour_ads_click = "last_hour_ads_click"
  //创建一个时间对象
  private val sdf = new SimpleDateFormat("HH:mm")


  /**
    * 统计最近一小时广告点击量并将数据写入Redis
    * @param filterAdsLogDStream 过滤后的数据集
    */
  def saveLastHourAdClickCountToRedis(filterAdsLogDStream: DStream[AdsLog]) = {

    //1.map=>((ad,hh:mm),1L)

    val adHourMinToOne: DStream[((String, String), Long)] = filterAdsLogDStream.map(adsLog => {
      val date: String = sdf.format(new Date(adsLog.timestamp))
      ((adsLog.adid, date), 1L)
    })


    //2.reducerByKeyAndWindow(_+_,,)=>((ad,hh:mm),count)
    val adHourMinToCount: DStream[((String, String), Long)] = adHourMinToOne.reduceByKeyAndWindow((x: Long, y: Long) => x + y, Minutes(3))


    //3.map=>(ad,(hh:mm,count))
    val adToHourMinCount: DStream[(String, (String, Long))] = adHourMinToCount.map { case ((ad, hourMin), count) =>
      (ad, (hourMin, count))
    }

    //4.groupByKey=>(ad,Itre[(hh:mm,count)...])
    val adToHourMinCountList: DStream[(String, Iterable[(String, Long)])] = adToHourMinCount.groupByKey()

    //5.mapValues=>(ad,jsonStr)
    val adToHourMinCountStr: DStream[(String, String)] = adToHourMinCountList.mapValues(items => {

      import org.json4s.JsonDSL._
      JsonMethods.compact(items.toList)
    })



    //6.写入Redis
    adToHourMinCountStr.foreachRDD(rdd =>{

      //获取redis连接
      val jedis: Jedis = RedisUtil.getJedisClient

      //判断清楚过期数据
      if(jedis.exists(last_hour_ads_click)){
        jedis.del(last_hour_ads_click)
      }
      jedis.close()

      rdd.foreachPartition(items =>{
        //将数据保持到redis

        //方式一：一条一条存
//        items.foreach { case (ad, jsonStr) =>
//            jedis.hset(last_hour_ads_click,ad,jsonStr)
//        }

        //方式二：批量插入数据
        //判断是否为空
        if(items.nonEmpty){
          val jedis: Jedis = RedisUtil.getJedisClient

          import scala.collection.JavaConversions._
          jedis.hmset(last_hour_ads_click,items.toMap)

          //关闭连接
          jedis.close()

        }

      })

    })

  }

}
