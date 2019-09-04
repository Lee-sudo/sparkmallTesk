package com.atguigu.app

import java.util.Properties

import com.atguigu.udf.CityRatioUDAF
import com.atguigu.utils.PropertiesUtil
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AreaProductTop3App {

  def main(args: Array[String]): Unit = {
    //1.获取SparkSession对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("AreaProductTop3App")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    //注册UDAF
    spark.udf.register("CityRatio",new CityRatioUDAF)

    //2.读取数据
    spark.sql("select c.area,p.product_name,c.city_name from user_visit_action u join city_info c on u.city_id=c.city_id join product_info p on u.click_product_id=p.product_id").createOrReplaceTempView("area_product_tmp")

    //3.计算点击总数以及城市占比
    spark.sql("select area,product_name,count(*) ct,CityRatio(city_name) cityRatio from area_product_tmp group by area,product_name").createOrReplaceTempView("area_product_count_tmp")

    //4.排名
    spark.sql("select area,product_name,ct,cityRatio,rank() over(partition by area order by ct desc) rk from area_product_count_tmp").createOrReplaceTempView("area_product_count_rk_tmp")

    //5.取前3名
    val df: DataFrame = spark.sql(s"select 'aaa--${System.currentTimeMillis()}' task_id ,area,product_name,ct product_count,cityRatio city_click_ratio from area_product_count_rk_tmp where rk<=3")

    //获取配置文件信息
    val properties: Properties = PropertiesUtil.load("config.properties")

    //6.写入MySQL
    df.write.format("jdbc")
      .option("url", properties.getProperty("jdbc.url"))
      .option("user", properties.getProperty("jdbc.user"))
      .option("password", properties.getProperty("jdbc.password"))
      .option("dbtable", "area_count_info")
      .mode(SaveMode.Append)
      .save()

    //7.关闭
    spark.close()

  }

}
