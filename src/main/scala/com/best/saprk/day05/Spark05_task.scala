package com.best.saprk.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/13
  * Desc:  Job调度 以及Task划分
  */
object Spark05_task {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,1,2),2)

    //聚合
    val resultRDD: RDD[(Int, Int)] = dataRDD.map((_,1))

    // Job：一个Action算子就会生成一个Job；
    //job1打印到控制台
    resultRDD.collect().foreach(println)

    //job2输出到磁盘
    resultRDD.saveAsTextFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\output")

    Thread.sleep(10000000)
    // 关闭连接
    sc.stop()
  }
}
