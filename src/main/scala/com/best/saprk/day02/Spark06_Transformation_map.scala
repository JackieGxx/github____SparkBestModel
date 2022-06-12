package com.best.saprk.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 
  */
object Spark06_Transformation_map {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    println("原分区个数：" + rdd.partitions.size)

    val newRDD: RDD[Int] = rdd.map(_*2)
    println("--------------------------------")
    println("新分区个数：" + newRDD.partitions.size)


    newRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
