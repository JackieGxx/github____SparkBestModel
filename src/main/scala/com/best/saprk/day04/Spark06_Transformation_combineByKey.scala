package com.best.saprk.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-combineByKey
  *
  */
object Spark06_Transformation_combineByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //需求：求出每一个学生的平均成绩
    //创建RDD
    val scoreRDD: RDD[(String, Int)] = sc.makeRDD(List(("jingjing",90),("jiafeng",60),("jingjing",96),("jiafeng",62),("jingjing",100),("jiafeng",50)))

    /*
    //方案1
    //(jingjing,CompactBuffer(90, 96, 100))
    val groupRDD: RDD[(String, Iterable[Int])] = scoreRDD.groupByKey()

    //如果分组之后某个组数据量比较大  会造成单点压力
    val resRDD: RDD[(String, Int)] = groupRDD.map {
      case (name, scoreSeq) => {
        (name, scoreSeq.sum / scoreSeq.size)
      }
    }
    */

    /*
    //方案2  使用reduceByKey
    // (name,score)==>(name,(score,1))
    //对RDD中的数据进行结构的转换       (jingjing,(96,1))
    val mapRDD: RDD[(String, (Int, Int))] = scoreRDD.map {
      case (name, score) => {
        (name, (score, 1))
      }
    }

    //通过reduceByKey对当前学生成绩进行聚合
    //(jingjing,(96,1))
    //(jingjing,(100,1))  ==>(jingjing,(196,2))
    val reduceRDD: RDD[(String, (Int, Int))] = mapRDD.reduceByKey {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    }

    //求出平均值
    val resRDD: RDD[(String, Int)] = reduceRDD.map {
      case (name, (score, count)) => {
        (name, score / count)
      }
    }
    resRDD.collect().foreach(println)
    */
    //方案3：通过combineByKey算子
    //createCombiner: V => C,     对RDD中当前key取出第一个value做一个初始化
    //mergeValue: (C, V) => C,    分区内计算规则，主要在分区内进行，将当前分区的value值，合并到初始化得到的c上面
    //mergeCombiners: (C, C) => C 分区间计算规则
    // 0---("jingjing",90),("jingjing",95)
    // 1---("jingjing",100)
    val combineRDD: RDD[(String, (Int, Int))] = scoreRDD.combineByKey(
      (_, 1),
      (t1: (Int, Int), v) => {
        (t1._1 + v, t1._2 + 1)
      },
      (t2: (Int, Int), t3: (Int, Int)) => {
        (t2._1 + t3._1, t2._2 + t3._2)
      }
    )

    //求平均成绩
    val resRDD: RDD[(String, Int)] = combineRDD.map {
      case (name, (score, count)) => {
        (name, score / count)
      }
    }

    resRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
