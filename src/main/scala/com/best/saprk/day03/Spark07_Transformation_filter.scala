package com.best.saprk.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-filter
      -按照指定的过滤规则，对RDD中的元素进行过滤
  */
object Spark07_Transformation_filter {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    /*
    val rdd: RDD[String] = sc.makeRDD(List("wangqiao","xiaojing","hanqi","chengjiang","xiaohao"))

    val newRDD: RDD[String] = rdd.filter(_.contains("xiao"))

    newRDD.collect().foreach(println)
    */


    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9),2)

    val newRDD: RDD[Int] = rdd.filter(_%2!=0)

    newRDD.collect().foreach(println)




    // 关闭连接
    sc.stop()
  }
}
