package com.atguigu.handler

import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object Top3DateAreaAdCountHandler {

  /**
    * 每天各地区 top3 热门广告并将数据保存至Redis
    * @param dateAreaCityAdToCount
    */
  def saveDateAreaAdCountToRedis(dateAreaCityAdToCount: DStream[((String, String, String, String), Long)]) = {

    //1.map=>((date,area,ad),count)
    //2.reducerByKey=>((date,area,ad),count)
    val dateAreaAdToCount: DStream[((String, String, String), Long)] = dateAreaCityAdToCount.map { case ((date, area, city, ad), count) =>
      ((date, area, ad), count)
    }.reduceByKey(_ + _)

    //3.map=>((date,area),(ad,count))
    val dateAreaToAdCount: DStream[((String, String), (String, Long))] = dateAreaAdToCount.map { case ((date, area, ad), count) =>
      ((date, area), (ad, count))
    }

    //4.groupByKey=>((date,area),Iter[(ad,count)])
    val dateAreaToAdCountList: DStream[((String, String), Iterable[(String, Long)])] = dateAreaToAdCount.groupByKey()

    //5.mapValues=>((date,area),Iter[(ad,count)])排序&取前3
    val sortedDateAreaToAdCountTop3: DStream[((String, String), List[(String, Long)])] = dateAreaToAdCountList.mapValues(items => {
      items.toList.sortWith(_._2 > _._2).take(3)
    })


    //6.将前3名点击次数的List转换为JSON
    val dateAreaToJson: DStream[((String, String), String)] = sortedDateAreaToAdCountTop3.mapValues(items => {
      //添加json 隐式转换   导包 import org.json4s.jackson.JsonMethods
      import org.json4s.JsonDSL._
      JsonMethods.compact(items)
    })


    //7.将结果数据存入Redis
    dateAreaToJson.foreachRDD(rdd =>{
      rdd.foreachPartition(items =>{

        //获取redis连接
        val jedis: Jedis = RedisUtil.getJedisClient

        //方案一:单条数据添加至redis
        items.foreach{case((date,area),jsonStr) =>
           val redisKey = s"top3_ads_per_day:$date"
            jedis.hset(redisKey,area,jsonStr)
        }

        //方案二：批量插入数据  优点：减少redis连接次数
        val dateTodateAreaJsonMap: Map[String, Map[(String, String), String]] = items.toMap.groupBy(_._1._1)

        val dateToAreaJsonMap: Map[String, Map[String, String]] = dateTodateAreaJsonMap.mapValues(items => {
          items.map { case ((_, area), jsonStr) => (area, jsonStr) }
        })

        dateToAreaJsonMap.foreach{case (date,map) =>
          var redisKey = s"top3_ads_per_day:$date"

          import scala.collection.JavaConversions._
          jedis.hmset(redisKey,map)
        }

        //关闭连接
        jedis.close()

      })


    })

  }

}
