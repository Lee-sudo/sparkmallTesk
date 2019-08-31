package com.atguigu.handle

import com.alibaba.fastjson.JSONObject
import com.atguigu.datamode.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object CategaryTop10AppHandler {

  /**
    * 读取hive数据
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
    sql

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
