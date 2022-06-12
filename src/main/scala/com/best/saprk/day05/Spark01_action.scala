package com.best.saprk.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/13
  * Desc:  行动算子
  */
object Spark01_action {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //val rdd: RDD[Int] = sc.makeRDD(List(1,6,5,3,2),2)
    /*
    //reduce
    val res: Int = rdd.reduce(_+_)
    println(res)
    */

    /*
    //collect
    val ints: Array[Int] = rdd.collect()
    ints.foreach(println)

    //foreach
    rdd.foreach(println)
    */

    /*
   //count 获取RDD中元素的个数
    val res: Long = rdd.count()
    println(res)

    // first 返回RDD中的第一个元素
    val res: Int = rdd.first()
    println(res)

    //take   返回rdd前n个元素组成的数组
    val ints: Array[Int] = rdd.take(3)
    println(ints.mkString(","))

    //takeOrdered  获取RDD排序后  前n的元素组成的数组
    val ints: Array[Int] = rdd.takeOrdered(3)
    println(ints.mkString(","))

    //aggregate
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),8)
    //val res: Int = rdd.aggregate(0)(_+_,_+_)//10
    //val res: Int = rdd.aggregate(10)(_+_,_+_)//100
    //fold是aggregate的简化，分区内和分区间计算规则相同
    val res: Int = rdd.fold(10)(_+_)
    println(res)

    //countByKey  统计每种key出现的次数
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))

    val res: collection.Map[Int, Long] = rdd.countByKey()

    println(res)
    */

    //save相关的算子
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    //保存为文本文件
    rdd.saveAsTextFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\output")
    //保存序列化文件
    rdd.saveAsObjectFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\output1")
    //保存为SequenceFile   注意：只支持kv类型RDD
    rdd.map((_,1)).saveAsSequenceFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\outpu2")

    // 关闭连接
    sc.stop()
  }
}
