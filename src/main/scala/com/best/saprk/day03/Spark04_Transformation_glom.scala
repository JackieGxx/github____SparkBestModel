package com.best.saprk.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-glom
      -将RDD一个分区中的元素，组合成一个新的数组
  */
object Spark04_Transformation_glom {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),2)

    println("------------没有glom之前------------")
    rdd.mapPartitionsWithIndex{
      (index,datas)=>{
        println(index + "--->" + datas.mkString(","))
        datas
      }
    }.collect()

    println("------------调用glom之后------------")
    val newRDD: RDD[Array[Int]] = rdd.glom()
    newRDD.mapPartitionsWithIndex{
      (index,datas)=>{
        println(index + "--->" + datas.next().mkString(","))
        datas
      }
    }.collect()



    // 关闭连接
    sc.stop()
  }
}
