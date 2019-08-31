package com.atguigu.utils
import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource


object JdbcUtil {

  var dataSource: DataSource = init()

  //德鲁伊的连接池方法，获取数据库连接
  def init(): DataSource = {

    val properties = new Properties()

    val config: Properties = PropertiesUtil.load("config.properties")

    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", config.getProperty("jdbc.url"))
    properties.setProperty("username", config.getProperty("jdbc.user"))
    properties.setProperty("password", config.getProperty("jdbc.password"))
    properties.setProperty("maxActive", config.getProperty("jdbc.datasource.size"))

    DruidDataSourceFactory.createDataSource(properties)
  }

  /**
    * 单条数据
    * @param sql
    * @param params
    * @return
    */
  def executeUpdate(sql: String, params: Array[Any]): Int = {
    var rtn = 0
    var pstmt: PreparedStatement = null
    val connection: Connection = dataSource.getConnection
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  /**
    * 多条数据
    * @param sql
    * @param paramsList
    * @return
    */
  def executeBatchUpdate(sql: String, paramsList: Iterable[Array[Any]]): Array[Int] = {
    var rtn: Array[Int] = null
    var pstmt: PreparedStatement = null
    val connection: Connection = dataSource.getConnection
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      for (params <- paramsList) {
        if (params != null && params.length > 0) {
          for (i <- params.indices) {
            pstmt.setObject(i + 1, params(i))
          }
          pstmt.addBatch()
        }
      }
      rtn = pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  def main(args: Array[String]): Unit = {
    JdbcUtil.executeUpdate("insert into category_top10 values(?,?,?,?,?)", Array("aaa", "12", 10, 12, 8))
  }


}
