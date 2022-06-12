package com.best.saprk.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-reduceByKey
  *   -根据相同的key对RDD中的元素进行聚合
  */
object Spark02_Transformation_reduceByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",3),("a",5),("b",2)))

    val resRDD: RDD[(String, Int)] = rdd.reduceByKey(_+_)

    resRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
