package com.best.saprk.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-distinct
  *   去重
  */
object Spark09_Transformation_distinct {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    //创建RDD
    val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,5,4,3,3),5)

    numRDD.mapPartitionsWithIndex{
      (index,datas)=>{
        println(index + "--->" + datas.mkString(","))
        datas
      }
    }.collect()

    println("---------------")

    //对RDD中的数据进行去重
    val newRDD: RDD[Int] = numRDD.distinct()

    newRDD.mapPartitionsWithIndex{
      (index,datas)=>{
        println(index + "--->" + datas.mkString(","))
        datas
      }
    }.collect()

    //newRDD.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
