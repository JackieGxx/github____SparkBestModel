package com.best.saprk.day08

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Author: Felix
  * Date: 2020/5/18
  * Desc:   演示RDD&DF&DS之间关系以及转换
  */
object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL01_Demo").setMaster("local[*]")

    //创建SparkSQL执行的入口点对象  SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //注意：spark不是包名，也不是类名，是我们创建的入口对象SparkSession的对象名称
    import spark.implicits._

    //读取json文件创建DataFrame
    //val df: DataFrame = spark.read.json("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\input\\test.json")

    //查看df里面的数据
    //df.show()

    //SQL语法风格
    //df.createOrReplaceTempView("user")
    //spark.sql("select * from user").show()

    //DSL风格
    //df.select("name","age").show()

    //RDD--->DataFrame--->DataSet
    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"banzhang",20),(2,"jingjing",18),(3,"wangqiang",30)))

    //RDD--->DataFrame
    val df: DataFrame = rdd.toDF("id","name","age")

    //DataFrame--->DataSet
    val ds: Dataset[User] = df.as[User]

    //DataSet--->DataFrame--->RDD
    val df1: DataFrame = ds.toDF()

    val rdd1: RDD[Row] = df1.rdd

    ds.show()

    //释放资源
    spark.stop()
  }
}

case class User(id:Int,name:String,age:Int)
