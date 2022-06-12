package com.best.saprk.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-groupByKey
  *   -根据key对RDD中的元素进行分组
  */
object Spark03_Transformation_groupByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

   //创建RDD
    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)))

    //(a,CompactBuffer((a,1), (a,5)))
    //val groupRDD: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(t=>t._1)

    //根据key对RDD进行分组       (a,CompactBuffer(1, 5))
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    val resRDD: RDD[(String, Int)] = groupRDD.map {
      case (key, datas) => {
        (key, datas.sum)
      }
    }

    resRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
