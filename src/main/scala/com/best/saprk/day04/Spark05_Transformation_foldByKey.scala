package com.best.saprk.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-foldByKey
  *   -foldByKey是AggregateByKey简化版本
  *   -foldByKey(初始值)(分区内和间计算规则)
  */
object Spark05_Transformation_foldByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    //如果分区内和分区间计算规则一样，并且不需要指定初始值，那么优先使用reduceByKey
    //val resRDD: RDD[(String, Int)] = rdd.reduceByKey(_+_)

    //如果分区内和分区间计算规则一样，并且需要指定初始值，那么优先使用foldByKey
    //val resRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)

    //如果分区内和分区间计算规则不一样，并且需要指定初始值，那么优先使用aggregateByKeyByKey
    val resRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)

    resRDD.collect().foreach(println)


    // 关闭连接
    sc.stop()
  }
}
