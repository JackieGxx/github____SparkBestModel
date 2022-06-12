package com.best.saprk.day06

import java.sql.DriverManager

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Author: Felix
  * Date: 2020/5/15
  * Desc: 从MySQL数据库中读取数据
    sc: SparkContext,   Spark程序执行的入口，上下文对象
    getConnection: () => Connection,  获取数据库连接
    sql: String,  执行SQL语句
    lowerBound: Long, 查询的其实位置
    upperBound: Long, 查询的结束位置
    numPartitions: Int,分区数
    mapRow: (ResultSet) => T   对结果集的处理

    注册驱动
    获取连接
    创建数据库操作对象PrepareStatement
    执行SQL
    处理结果集
    关闭连接
  */
object Spark04_MySQL_read {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口 
    val sc: SparkContext = new SparkContext(conf)

    //数据库连接4要素
    var driver = "com.mysql.jdbc.Driver"
    var url = "jdbc:mysql://hadoop202:3306/test"
    var username = "root"
    var password = "123456"

    var sql:String = "select * from user where id >= ? and id <= ?"
    val resRDD = new JdbcRDD(
      sc,
      () => {
        //注册驱动
        Class.forName(driver)
        //获取连接
        DriverManager.getConnection(url, username, password)
      },
      sql,
      1,
      20,
      2,
      rs => (rs.getInt(1), rs.getString(2), rs.getInt(3))
    )

    resRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
