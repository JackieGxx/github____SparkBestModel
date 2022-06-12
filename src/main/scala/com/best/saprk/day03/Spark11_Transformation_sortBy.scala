package com.best.saprk.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-sortBy
  *   对RDD中的元素进行排序
  *
  */
object Spark11_Transformation_sortBy {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    //val numRDD: RDD[Int] = sc.makeRDD(List(1,4,3,2))

    //升序排序
    //val sortedRDD: RDD[Int] = numRDD.sortBy(num=>num)
    //降序排序
    //val sortedRDD: RDD[Int] = numRDD.sortBy(num=> -num)
    //val sortedRDD: RDD[Int] = numRDD.sortBy(num=> num,false)

    val strRDD: RDD[String] = sc.makeRDD(List("1","4","3","22"))

    //按照字符串字符字典顺序进行排序
    //val sortedRDD: RDD[String] = strRDD.sortBy(elem=>elem)

    //按照字符串转换为整数后的大小进行排序
    val sortedRDD: RDD[String] = strRDD.sortBy(_.toInt)

    sortedRDD.collect().foreach(println)

    Thread.sleep(1000000)

    // 关闭连接
    sc.stop()
  }
}
