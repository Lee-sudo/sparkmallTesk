package com.atguigu.handle

import com.alibaba.fastjson.JSONObject
import com.atguigu.datamode.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object CategaryTop10AppHandler {

  /**
    *
    * @param userVisitActionRDD
    * @param categoryTop10
    */
  def getCategoryTop10Session(userVisitActionRDD: RDD[UserVisitAction], categoryTop10: List[(String, mutable
  .HashMap[String, Long])]) = {

    //1.读取数据(已做)userVisitAction[click(1,2,3,7,8,9),order,pay,serach]

    //2.过滤数据(过滤出点击品类在1,2,3的数据)userVisitAction[click(1,2,3)]
    val categoryList: List[String] = categoryTop10.map(_._1)
    val filterdUserVistiActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(UserVisitAction => {
      categoryList.contains(UserVisitAction.click_category_id.toString)
    })
    

    //3.转换：userVisitAction=>((category,sessionId),1L)
    val categoryAndSessionToOne: RDD[((String, String), Long)] = filterdUserVistiActionRDD.map(UserVisitAction =>
      ((UserVisitAction.click_category_id.toString, UserVisitAction.session_id), 1L)
    )
    

    //4.求和：((category,sessionId),1L)=>((category,sessionId),sum)
    val categoryAndSessionToSum: RDD[((String, String), Long)] = categoryAndSessionToOne.reduceByKey(_ + _)
    
    //5.转换维度：((category,sessionId),sum)=>(category,(sessionId,sum))
    val categoryAndSessionAndSum: RDD[(String, (String, Long))] = categoryAndSessionToSum.map {
      case ((category, sessionId), sum) => (category, (sessionId, sum))
    }
    

    //6.分组(1,[(s1,20),(s2,50)...]
    val categoryToSessionAndSumItr: RDD[(String, Iterable[(String, Long)])] = categoryAndSessionAndSum.groupByKey()

    //7.排序取前十
    val sortedcategoryToSessionAndSumItr: RDD[(String, List[(String, Long)])] = categoryToSessionAndSumItr.mapValues(
      itr => itr.toList.sortWith(_._2 > _._2).take(10)
    )


    //8.压平
    val result: RDD[(String, String, Long)] = sortedcategoryToSessionAndSumItr.flatMap {
      case (category, itr) =>
        val tuples: List[(String, String, Long)] = itr.map {
          case (session, sum) => (category, session, sum)
        }
        tuples
    }

    //9.返回
    result

  }


  /**
    * 读取hive数据
    *
    * @param spark
    * @param jsonObj
    * @return
    */
  def readHiveData(spark: SparkSession, jsonObj: JSONObject): RDD[UserVisitAction] = {

    //隐式转换
    import spark.implicits._

    //1.准备过滤条件
    val startDate: String = jsonObj.getString("startDate")
    val endDate: String = jsonObj.getString("endDate")
    val startAge: String = jsonObj.getString("startAge")
    val endAge: String = jsonObj.getString("endAge")

    //2.封装sql语句
    val sql = new StringBuilder("select ac.* from user_visit_action ac join user_info u on ac.user_id=u.user_id where" +
      " 1=1")


    //3.拼接sql

    if(startDate != null){
      sql.append(s" and date >= '$startDate' ")
    }
    if(endDate != null){
      sql.append(s" and date <= '$endDate' ")
    }
    if(startAge != null){
      sql.append(s" and age >= $startAge")
    }
    if(endAge != null){
      sql.append(s" and age <= $endAge")
    }

    //4.打印sql
    println(sql.toString())

    //5.读取数据
    val df: DataFrame = spark.sql(sql.toString())


    //6.转换为RDD[UserVisitAction]
    val rdd: RDD[UserVisitAction] = df.as[UserVisitAction].rdd

    //7.返回结果
    rdd

  }


}
