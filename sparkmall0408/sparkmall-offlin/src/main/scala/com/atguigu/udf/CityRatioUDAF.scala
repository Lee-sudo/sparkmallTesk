package com.atguigu.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class CityRatioUDAF extends UserDefinedAggregateFunction{
  //北京21.2%，天津13.2%，其他65.6%

  //输入数据类型
  override def inputSchema: StructType = StructType(StructField("city",StringType) :: Nil)

  //中间数据类型
  override def bufferSchema: StructType = StructType(StructField("cityCount", MapType(StringType, LongType)) :: StructField("totalCount", LongType) :: Nil)

  //输出数据类型
  override def dataType: DataType = StringType

  //函数稳定性
  override def deterministic: Boolean = true

  //缓存数据类型的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String,Long]()
    buffer(1) = 0L
  }

  //区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //取出buffer中的数据
    val cityCount: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getLong(1)

    val city: String = input.getString(0)

    //赋值
    buffer(0) = cityCount + (city -> (cityCount.getOrElse(city, 0L) + 1L))

    buffer(1) = totalCount + 1L
  }

  //区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //取出缓存的值
    val cityCount1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    val totalCount1: Long = buffer1.getLong(1)

    val cityCount2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)
    val totalCount2: Long = buffer2.getLong(1)


    //赋值
    buffer1(0) = cityCount1.foldLeft(cityCount2) { case (map, (city, count)) =>
      map + (city -> (map.getOrElse(city, 0L) + count))
    }

    buffer1(1) = totalCount1 + totalCount2
  }

  //计算最终结果
  override def evaluate(buffer: Row): Any = {
    //北京21.2%，天津13.2%，其他65.6%

    //1.取值
    val cityCount: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getLong(1)

    //2.对城市点击次数进行排序并取前2名
    val sortedCityCount: List[(String, Long)] = cityCount.toList.sortWith(_._2 > _._2).take(2)

    var otherRatio = 100D

    //3.计算城市比率
    val cityRatioTop2: List[String] = sortedCityCount.map { case (city, count) =>
      val ratio: Double = Math.round(count.toDouble * 1000 / totalCount) / 10D
      otherRatio -= ratio
      s"$city:$ratio%"
    }

    //4.增加其他城市
    val cityRatio: List[String] = cityRatioTop2 :+ s"其他:${Math.round(otherRatio * 10) / 10D}%"

    //5.转换为字符串
    cityRatio.mkString(",")


  }
}
