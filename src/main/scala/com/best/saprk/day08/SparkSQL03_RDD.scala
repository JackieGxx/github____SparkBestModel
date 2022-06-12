package com.best.saprk.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/18
  * Desc: 求平均年龄----RDD算子方式实现
  */
object SparkSQL03_RDD {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建一个RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("jingjing",20),("bangzhang",40),("xuchong",30)))

    //转换结构
    val mapRDD: RDD[(Int, Int)] = rdd.map {
      case (name, age) => {
        (age, 1)
      }
    }
    //对年龄以及总人数进行聚合操作   (ageSum,countSum)
    val res: (Int, Int) = mapRDD.reduce {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    }

    println(res._1 / res._2)

    // 关闭连接
    sc.stop()
  }
}
