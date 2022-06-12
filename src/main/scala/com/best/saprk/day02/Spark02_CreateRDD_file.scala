package com.best.saprk.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc:  通过读取外部文件，创建RDD
  */
object Spark02_CreateRDD_file {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //从本地文件中读取数据，创建RDD
    //val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\input\\1.txt")


    //从HDFS服务器上读取数据，创建RDD
    val rdd: RDD[String] = sc.textFile("hdfs://hadoop202:8020/input")

    rdd.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
