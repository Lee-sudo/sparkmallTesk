package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.AdsLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListHandler {

  //定义一个时间类转换
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  //定义黑名单的RedisKey
  private val blackList:String ="BlackList"
  /**
    * 校验数据集
    * @param adsLogDStream  过滤后的数据集
    */
  def checkDataToBlickList(adsLogDStream: DStream[AdsLog]) = {

    //1.单日每个人点击每个广告的次数
    val dayeUserAdToCount: DStream[((String, String, String), Long)] = adsLogDStream.map(adsLog => {
      //获取年月日
      val date: String = sdf.format(new Date(adsLog.timestamp))

      ((date, adsLog.userid, adsLog.adid), 1L)

    }).reduceByKey(_ + _)


    //2.将点击此时数据集和redis中现有的数据统计总数（50+5）
    dayeUserAdToCount.foreachRDD(rdd =>{
      //用foreachPartition代替foreach 好处： 一个分区进行一次链接操作，
      rdd.foreachPartition(items => {
        //a.获取redis连接
        val jedis: Jedis = RedisUtil.getJedisClient

        items.foreach { case (((date, userid, adid), count)) =>
            //拼接rediskey
          val redisKey = s"date_user_ad_$date"
            val hashKey = s"$userid:$adid"
          //将数据进行汇总
            jedis.hincrBy(redisKey,hashKey,count)

          if(jedis.hget(redisKey,hashKey).toLong >= 50L){
            //将该用户加入到黑名单
            jedis.sadd(blackList,userid)
          }


        }

        //关闭链接
        jedis.close()

      })
    })

    //3.校验点击次数，超过100，则加入黑名单

  }

  /**
    * 根据黑名单过滤数据集
    * @param sparkContext
    * @param adsLogDStream 原始数据
    */
  def filterDataByBlackList(sc: SparkContext, adsLogDStream: DStream[AdsLog]): DStream[AdsLog] = {

    adsLogDStream.transform(rdd =>{

      //1.获取redis连接
      val jedis: Jedis = RedisUtil.getJedisClient

      //2.获取黑名单数据
      val userIds: util.Set[String] = jedis.smembers(blackList)

      //3.关闭redis连接
      jedis.close()

      //4.将黑名单数据进行广播
      val userIdsBC: Broadcast[util.Set[String]] = sc.broadcast(userIds)

     rdd.filter(adsLog => !userIdsBC.value.contains(adsLog.userid))

    })

  }

}
