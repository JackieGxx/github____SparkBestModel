package com.best.saprk.day08

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: Felix
  * Date: 2020/5/18
  * Desc:   自定义UDF函数，在每一个查询的名字前，加问候语
  */
object SparkSQL02_UDF {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL02_UDF")
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //创建DF
    val df: DataFrame = spark.read.json("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\input\\test.json")


    //注册自定义函数
    spark.udf.register("addSayHi",(name:String)=>{"nihao:" + name})

    //创建临时视图
    df.createOrReplaceTempView("user")

    //通过SQL语句，从临时视图查询数据
    spark.sql("select addSayHi(name) as newname,age from user").show

    //释放资源
    spark.stop()
  }
}
