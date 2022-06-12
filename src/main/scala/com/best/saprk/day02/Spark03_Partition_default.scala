package com.best.saprk.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 默认分区
  *   -从集合中创建RDD
  *       取决于分配给应用的CPU的核数
  *   -读取外部文件创建RDD
  *       math.min(取决于分配给应用的CPU的核数,2)
  */
object Spark03_Partition_default {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //通过集合创建RDD
    //val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    //通过读取外部文件创建RDD
    val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\input")
    //查看分区效果
    //println(rdd.partitions.size)
    rdd.saveAsTextFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\output")

    // 关闭连接
    sc.stop()
  }
}
