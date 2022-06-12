package com.best.saprk.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 通过读取内存集合中的数据，创建RDD
  */
object Spark01_CreateRDD_mem {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 创建一个集合对象
    val list: List[Int] = List(1,2,3,4)

    //根据集合创建RDD 方式一
    //val rdd: RDD[Int] = sc.parallelize(list)

    //根据集合创建RDD 方式二   底层调用的就是parallelize(seq, numSlices)
    val rdd: RDD[Int] = sc.makeRDD(list)

    rdd.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
