package com.best.saprk.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 从集合中创建RDD指定分区
  *   - 默认分区规则
  *       取决于分配给应用的CPU的核数
  *   - 指定分区数
  *
  */
object Spark04_Partition_mem {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //通过集合创建RDD
    //集合中4个数据---默认分区数---实际输出12个分区---分区中数据分布   2分区->1,5分区->2,8分区->3,11分区->4
    //val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    //集合中4个数据---设置分区数为4---实际输出4个分区---分区中数据分布   0分区->1, 1分区->2, 2分区->3, 3分区->4
    //val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),4)

    //集合中4个数据---设置分区数为3---实际输出3个分区---分区中数据分布   0分区->1, 1分区->2, 3分区->3 4
    //val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),3)

    //集合中5个数据---设置分区数为3---实际输出3个分区---分区中数据分布   0分区->1, 1分区->2, 2分区->3, 3分区->4
    val rdd: RDD[String] = sc.makeRDD(List("a","b","c","d","e"),3)

    rdd.saveAsTextFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\output")

    // 关闭连接
    sc.stop()
  }
}
