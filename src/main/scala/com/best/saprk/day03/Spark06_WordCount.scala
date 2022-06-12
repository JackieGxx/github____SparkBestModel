package com.best.saprk.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/11
  * Desc: 使用groupBy完成WordCount案例
  */
object Spark06_WordCount {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)



   /*
    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"))
   //简单版-实现方式1
    //对RDD中的元素进行扁平映射
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))

    //将映射后的数据进行结构的转换，为每个单词计数
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_,1))

    //按照key对RDD中的元素进行分组   (Hello,CompactBuffer((Hello,1), (Hello,1), (Hello,1)))
    val groupByRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupBy(_._1)

    //对分组后的元素再次进行映射  (Hello,3)
    val resRDD: RDD[(String, Int)] = groupByRDD.map {
      case (word, datas) => {
        (word, datas.size)
      }
    }


    //简单版-实现方式2
    //对RDD中的元素进行扁平映射
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))

    //将RDD中的单词进行分组
    val groupByRDD: RDD[(String, Iterable[String])] = flatMapRDD.groupBy(word=>word)

    //对分组之后的数据再次进行映射
    val resRDD: RDD[(String, Int)] = groupByRDD.map {
      case (word, datas) => {
        (word, datas.size)
      }
    }

  */



    /*
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("Hello Scala", 2), ("Hello Spark", 3), ("Hello World", 2)))
    //复杂版 方式1
    //将原RDD中字符串以及字符串出现的次数，进行处理，形成一个新的字符串
    val rdd1: RDD[String] = rdd.map {
      case (str, count) => {
        (str + " ") * count
      }
    }

    //对RDD中的元素进行扁平映射
    val flatMapRDD: RDD[String] = rdd1.flatMap(_.split(" "))

    //将RDD中的单词进行分组
    val groupByRDD: RDD[(String, Iterable[String])] = flatMapRDD.groupBy(word=>word)

    //对分组之后的数据再次进行映射
    val resRDD: RDD[(String, Int)] = groupByRDD.map {
      case (word, datas) => {
        (word, datas.size)
      }
    }*/


    //复杂版  方式2        ("Hello Scala", 2)==>(hello,2),(Scala,2)     ("Hello Spark", 3)==>(hello,3),(Spark,3)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("Hello Scala", 2), ("Hello Spark", 3), ("Hello World", 2)))


    //对RDD中的元素进行扁平映射
    val flatMapRDD: RDD[(String, Int)] = rdd.flatMap {
      case (words, count) => {
        words.split(" ").map(word => (word, count))
      }
    }

    //按照单词对RDD中的元素进行分组     (Hello,CompactBuffer((Hello,2), (Hello,3), (Hello,2)))
    val groupByRDD: RDD[(String, Iterable[(String, Int)])] = flatMapRDD.groupBy(_._1)

    //对RDD的元素重新进行映射
    val resRDD: RDD[(String, Int)] = groupByRDD.map {
      case (word, datas) => {
        (word, datas.map(_._2).sum)
      }
    }

    resRDD.collect().foreach(println)


    // 关闭连接
    sc.stop()
  }
}
