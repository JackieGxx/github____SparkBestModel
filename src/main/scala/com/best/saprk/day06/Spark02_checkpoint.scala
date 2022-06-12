package com.best.saprk.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/15
  * Desc: RDD的检查点
  */
object Spark02_checkpoint {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //设置检查点目录
    sc.setCheckpointDir("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\cp")

    //开发环境，应该将检查点目录设置在hdfs上
    // 设置访问HDFS集群的用户名
    //System.setProperty("HADOOP_USER_NAME","atguigu")
    //sc.setCheckpointDir("hdfs://hadoop202:8020/cp")



    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello bangzhang","hello jingjing"),2)

    //扁平映射
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))

    //结构转换
    val mapRDD: RDD[(String, Long)] = flatMapRDD.map {
      word => {
        (word, System.currentTimeMillis())
      }
    }

    //打印血缘关系
    println(mapRDD.toDebugString)


    //在开发环境，一般检查点和缓存配合使用
    mapRDD.cache()

    //设置检查点
    mapRDD.checkpoint()

    //触发行动操作
    mapRDD.collect().foreach(println)

    println("---------------------------------------")

    //打印血缘关系
    println(mapRDD.toDebugString)

    //释放缓存
    //mapRDD.unpersist()

    //触发行动操作
    mapRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
