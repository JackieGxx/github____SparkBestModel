package com.best.saprk.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-重新分区
  *   -coalesce
  *     默认是不执行shuffle，一般用于缩减分区
  *   -repartition
  *     底层调用的就是coalesce，只不过默认是执行shuffle，一般用于扩大分区
  *
  */
object Spark10_Transformation_coalesce {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    //创建RDD
    val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)

    numRDD.mapPartitionsWithIndex{
      (index,datas)=>{
        println(index + "--->" + datas.mkString(","))
        datas
      }
    }.collect()

    println("********************************************")
    //缩减分区
    //val newRDD: RDD[Int] = numRDD.coalesce(2)
    //扩大分区
    //注意：默认情况下，如果使用coalesce扩大分区是不起作用的  。因为底层没有执行shuffle
    //val newRDD: RDD[Int] = numRDD.coalesce(4)

    //如果扩大分区    使用repartition
    val newRDD: RDD[Int] = numRDD.repartition(4)
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
