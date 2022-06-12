package com.best.saprk.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-aggregateByKey
  *   -按照key对分区内以及分区间的数据进行处理
  *   -aggregateByKey(初始值)(分区内计算规则,分区间计算规则)
  */
object Spark04_Transformation_aggregateByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    //reduceByKey实现wordCount
    //val resRDD: RDD[(String, Int)] = rdd.reduceByKey(_+_)

    //aggregateByKey实现wordCount
    //val resRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)

    rdd.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index + "------>" + datas.mkString(","))
        datas
      }
    ).collect()

    //分区最大值，求和
   /* val resRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (a, b) => a + b
    )*/


    //简化
    val resRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      math.max(_, _),
      _+_
    )
    resRDD.collect().foreach(println)


    // 关闭连接
    sc.stop()
  }
}
