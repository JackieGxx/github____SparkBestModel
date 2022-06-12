package com.best.saprk.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-groupBy
      -按照指定的规则，对RDD中的元素进行分组
  */
object Spark05_Transformation_groupBy {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9),2)
    println("==============groupBy分组前================")
    rdd.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index + "---->" + datas.mkString(","))
        datas
      }
    ).collect()

    val newRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(_%3)
    println("==============groupBy分组后================")
    newRDD.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index + "---->" + datas.mkString(","))
        datas
      }
    ).collect()
    /*
        val rdd: RDD[String] = sc.makeRDD(List("atguigu","hello","scala","atguigu","hello","scala"))

        val newRDD: RDD[(String, Iterable[String])] = rdd.groupBy(elem=>elem)

        newRDD.collect().foreach(println)
    */

    Thread.sleep(1000000)
    // 关闭连接
    sc.stop()
  }
}
