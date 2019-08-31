package com.atguigu.accu

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

class CategoryAccu extends AccumulatorV2 [String,mutable.HashMap[String,Long]]{

  private var categoryCount = new mutable.HashMap[String,Long]()
  //判空
  override def isZero: Boolean = categoryCount.isEmpty

  //复制
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {

    val accu = new CategoryAccu

    accu.categoryCount ++= this.categoryCount

    accu
  }

  //重置
  override def reset(): Unit = categoryCount.clear()

  //分区内添加单个值
  override def add(v: String): Unit = {
    categoryCount(v) =categoryCount.getOrElse(v,0L) + 1L

  }

  //分区间合并数据
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    //1.获取other的数据
  val otherCategoryCount: mutable.HashMap[String,Long] = other.value

    //2.便利赋值
    otherCategoryCount.foreach{
      case(category:String,count:Long) =>{
        this.categoryCount(category) = categoryCount.getOrElse(category,0L) + count
      }
    }

  }

  //返回结果
  override def value: mutable.HashMap[String, Long] = categoryCount
}
