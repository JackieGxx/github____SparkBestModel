package com.best.saprk.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Author: Felix
  * Date: 2020/5/15
  * Desc:  读取Json格式数据
  */
object Spark03_readJson {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\input\\test.json")

    val resRDD: RDD[Option[Any]] = rdd.map(JSON.parseFull)

    resRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
