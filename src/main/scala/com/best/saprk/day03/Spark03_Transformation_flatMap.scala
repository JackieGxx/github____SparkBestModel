package com.best.saprk.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-faltMap
      -对集合中的元素进行扁平化处理
  */
object Spark03_Transformation_flatMap {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1,2),List(3,4),List(5,6),List(7,8)),2)

    //注意：如果匿名函数输入和输出相同，那么不能简化
    val newRDD: RDD[Int] = rdd.flatMap(datas=>datas)

    newRDD.collect().foreach(println)


    // 关闭连接
    sc.stop()
  }
}
