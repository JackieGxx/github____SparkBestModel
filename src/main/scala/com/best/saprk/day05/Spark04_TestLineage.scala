package com.best.saprk.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/13
  * Desc: 查看血缘关系和依赖关系
  */
object Spark04_TestLineage {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello spark","hello jingjing"),2)

    //查看RDD的血缘关系
    println(rdd.toDebugString)
    //查看RDD的依赖关系
    println(rdd.dependencies)
    println("------------------------------")

    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))
    println(flatMapRDD.toDebugString)
    println(flatMapRDD.dependencies)
    println("------------------------------")

    val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_,1))
    println(mapRDD.toDebugString)
    println(mapRDD.dependencies)
    println("------------------------------")

    val resRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resRDD.toDebugString)
    println(resRDD.dependencies)
    println("------------------------------")


    // 关闭连接
    sc.stop()
  }
}
