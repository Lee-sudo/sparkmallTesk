package com.atguigu.app

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.accu.CategoryAccu
import com.atguigu.datamode.UserVisitAction
import com.atguigu.handle.CategaryTop10AppHandler
import com.atguigu.utils.{JdbcUtil, PropertiesUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object CategaryTop10App {

  def main(args: Array[String]): Unit = {


    //1.获取sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CategaryTop10App")
    //2.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("CategaryTop10App")
      .enableHiveSupport()
      .getOrCreate()

    //3.获取配置信息（获取session的范围）
    val properties: Properties = PropertiesUtil.load("conditions.properties")
    val jsonStr: String = properties.getProperty("condition.params.json")
    val jsonObj: JSONObject = JSON.parseObject(jsonStr)


    //4.按照要求读取数据(封装成方法)，注意返回值：RDD[UserVisitAction]
    val userVisitActionRDD:RDD[UserVisitAction] = CategaryTop10AppHandler.readHiveData(spark,jsonObj)

    //复用
    userVisitActionRDD.cache()

    //测试
//    userVisitActionRDD.foreach(UserVisitAction => println(s"${UserVisitAction.user_id}--${UserVisitAction.session_id}"))

    //5.创建累加器对象
    val accu = new CategoryAccu

    //6.注册累加器
    spark.sparkContext.register(accu,"categoryCount")

    //7.统计各个品类点击，下单及支付次数
    userVisitActionRDD.foreach(UserVisitAction =>{
      //a.判断是否为点击日志
      if(UserVisitAction.click_category_id != -1){
        accu.add(s"click_${UserVisitAction.click_category_id}")
      }else if(UserVisitAction.order_category_ids != null) {//b.判断是否时下单日志
        //c.遍历订单日志中的category个数
        UserVisitAction.order_category_ids.split(",").foreach(category =>
          accu.add(s"order_$category")
        )
      }else if(UserVisitAction.pay_category_ids != null){//d.判断是否时支付日志
        UserVisitAction.pay_category_ids.split(",").foreach(category =>
          accu.add(s"pay_$category")
        )
      }
    })

    //8.获取累加器中的数据:  (click_1,10)
    val categorySum: mutable.HashMap[String, Long] = accu.value

    //9.规整数据 规整后数据(1,(click_1,10),(order_1,8),(pary_1,5))
    val categoryToCategorySum: Map[String, mutable.HashMap[String, Long]] = categorySum.groupBy(_._1.split("_")(1))

    //10.排序，按照支付，订单及点击的顺序
    val categoryTop10: List[(String, mutable.HashMap[String, Long])] = categoryToCategorySum.toList.sortWith {
      case (c1, c2) => {
        //a.先获取比较双方的所需的数据 ：(1,(click_1,10),(order_1,8),(pary_1,5))
        val categorySum1: mutable.HashMap[String, Long] = c1._2
        val categorySum2: mutable.HashMap[String, Long] = c2._2

        //b.先比较支付次数
        if (categorySum1.getOrElse(s"pay_${c1._1}", 0L) > categorySum2.getOrElse(s"pay_${c2._1}", 0L)) {
          true
        } else if (categorySum1.getOrElse(s"pay_${c1._1}", 0L) == categorySum2.getOrElse(s"pay_${c2._1}", 0L)) {

          //c.再比较订单次数
          if (categorySum1.getOrElse(s"order_${c1._1}", 0L) > categorySum2.getOrElse(s"order_${c2._1}", 0L)) {
            true
          } else if (categorySum1.getOrElse(s"order_${c1._1}", 0L) == categorySum2.getOrElse(s"order_${c2._1}", 0L)) {

            //d.最后比较点击次数
            categorySum1.getOrElse(s"click_${c1._1}", 0L) > categorySum2.getOrElse(s"click_${c2._1}", 0L)

          } else {
            false
          }

        } else {
          false
        }

      }
    }.take(10)


    //11.
    val result: List[Array[Any]] = categoryTop10.map {
      case (category, categoryMap) => {
        Array(s"aaaa--${
          System.currentTimeMillis()
        }",
          category,
          categoryMap.getOrElse(s"click_$category", 0L),
          categorySum.getOrElse(s"order_$category", 0L),
          categorySum.getOrElse(s"pay_$category", 0L)

        )
      }
    }


    //12.写入到mysql数据库中
    JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)",result)


    //****************需求二：热门品类中活跃 Session*****************************************

    //
    val categoryAndSessionAndSum: RDD[(String, String, Long)] = CategaryTop10AppHandler.getCategoryTop10Session(userVisitActionRDD, categoryTop10)

    //转换数据格式
    val categoryAndSessionAndSumArr: RDD[Array[Any]] = categoryAndSessionAndSum.map { case (category, session, sum) =>
      Array(s"aaaa--${System.currentTimeMillis()}",
        category,
        session,
        sum)
    }

    //拉取到Driver
    val sessionTop10Arr: Array[Array[Any]] = categoryAndSessionAndSumArr.collect()

    //需求二保存至MySQL
    JdbcUtil.executeBatchUpdate("insert into category_session_top10 values(?,?,?,?)", sessionTop10Arr)

    //关闭连接
    spark.stop()
  }

}
