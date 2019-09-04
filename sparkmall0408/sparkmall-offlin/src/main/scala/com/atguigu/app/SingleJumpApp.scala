package com.atguigu.app

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.datamode.UserVisitAction
import com.atguigu.handle.SingleJumpHandler
import com.atguigu.utils.{JdbcUtil, PropertiesUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SingleJumpApp {

  def main(args: Array[String]): Unit = {

    //1.读取数据：userVisitAction
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SingleJumpApp")
      .enableHiveSupport()
      .getOrCreate()

    //隐式转换
    import spark.implicits._

    val userVisitActionRDD: RDD[UserVisitAction] = spark.sql("select * from user_visit_action").as[UserVisitAction].rdd

    ///2.读取配置文件：targetPageFlow:"1,2,3,4,5,6,7"并转换
    //“1,2,3,4,5,6”，“1-2,2-3...6-7”
    val properties: Properties = PropertiesUtil.load("conditions.properties")
    val jsonStr: String = properties.getProperty("condition.params.json")
    val jsonObj: JSONObject = JSON.parseObject(jsonStr)

    val targetPageFlowStr: String = jsonObj.getString("targetPageFlow")
    val targetPageFlowArr: Array[String] = targetPageFlowStr.split(",")

    //1,2,3,4,5,6
    val formPages: Array[String] = targetPageFlowArr.dropRight(1)
    //2,3,4,5,6,7
    val toPages: Array[String] = targetPageFlowArr.drop(1)

    val sigleJumpPages: Array[String] = formPages.zip(toPages).map {
      case (from, to) => s"$from-$to"
    }

    //3.过滤
    val singlePageSum: RDD[(String, Long)] = SingleJumpHandler.getSinglePageCount(userVisitActionRDD,formPages)

    val singleJumpCount: RDD[(String, Int)] = SingleJumpHandler.getSingleJumpCount(userVisitActionRDD,sigleJumpPages)

    //4.拉取数据到Driver计算跳转率
    val singlePageSumMap: Map[String, Long] = singlePageSum.collect().toMap
    val singleJumpCountArr: Array[(String, Int)] = singleJumpCount.collect()

    val result: Array[Array[Any]] = singleJumpCountArr.map { case (singleJump, count) =>
      val singleJumpRatio: Double = count.toDouble / singlePageSumMap.getOrElse(singleJump.split("-")(0), 1L)

      Array(s"aaaa--${System.currentTimeMillis()}", singleJump, singleJumpRatio)
    }




    //5.写入MySQL
    //JdbcUtil.executeBatchUpdate("insert into jump_page_ratio values(?,?,?)",result)

    JdbcUtil.executeBatchUpdate("insert into jump_page_ratio values(?,?,?)",result)

    //6.关闭
    spark.close()



  }

}
