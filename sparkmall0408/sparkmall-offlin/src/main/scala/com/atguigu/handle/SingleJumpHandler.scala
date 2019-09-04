package com.atguigu.handle

import com.atguigu.datamode.UserVisitAction
import org.apache.spark.rdd.RDD

object SingleJumpHandler {
  /**
    * 获取单跳次数
    * @param userVisitActionRDD 数据集
    * @param sigleJumpPages  过滤条件
    */
  def getSingleJumpCount(userVisitActionRDD: RDD[UserVisitAction], sigleJumpPages: Array[String]): RDD[(String, Int)]
  = {
    //1.转换格式(session,(time,page))
    val sessionToTimeAndPage: RDD[(String, (String, String))] = userVisitActionRDD.map(userVisitAction => (userVisitAction.session_id, (userVisitAction.action_time, userVisitAction.page_id.toString)))
    //2.分组
    val sessionToTimeAndPageItr: RDD[(String, Iterable[(String, String)])] = sessionToTimeAndPage.groupByKey()

    //3.排序
    val sortedSessionToTimeAndPageItr: RDD[(String, List[(String, String)])] = sessionToTimeAndPageItr.mapValues(items => {
      items.toList.sortWith(_._1 < _._1)
    })
    //4.过滤
    val filterSessionToPageItr: RDD[(String, List[String])] = sortedSessionToTimeAndPageItr.mapValues(items => {
      //a.去尾
      val fromPages: List[String] = items.map(_._2).dropRight(1)
      //b.去头
      val toPages: List[String] = items.map(_._2).drop(1)
      //c.拼接
      val jumPages: List[String] = fromPages.zip(toPages).map { case (from, to) => s"$from-$to" }
      //d.过滤
      val strings: List[String] = jumPages.filter(sigleJumpPages.contains(_))

      strings

    })

    println("******************")
    filterSessionToPageItr.collect().foreach(println)

    //5.压平并形成元组并计算
    val singleJumpCount: RDD[(String, Int)] = filterSessionToPageItr.flatMap(_._2).map((_,1)).reduceByKey(_ + _)

    //6.返回
    singleJumpCount

  }

  /**
    * 获取指定单个页面的总访问次数
    *
    * @param userVisitActionRDD
    * @param formPages
    */
  def getSinglePageCount(userVisitActionRDD: RDD[UserVisitAction], formPages: Array[String]): RDD[(String, Long)] = {

    //1.过滤数据集
    val fileterdUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(UserVisitAction =>
      formPages.contains(UserVisitAction.page_id.toString)
    )
    //2.转换格式
    val singlePageToOne: RDD[(String, Long)] = fileterdUserVisitActionRDD.map(UserVisitAction => (UserVisitAction.page_id.toString, 1L))
    //求和
    val singlePageToSum: RDD[(String, Long)] = singlePageToOne.reduceByKey(_ + _)

    //4.返回
    singlePageToSum


  }

}
