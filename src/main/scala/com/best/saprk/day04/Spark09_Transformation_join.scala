package com.best.saprk.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-join
  *   回顾SQL内容
  *     -SQL分类
  *       >按照年代分
  *         SQL92=>select * from emp e,dept d where e.deptno = d.deptno;
  *         SQL99=>select * from emp e join dept d on e.deptno = d.deptno;
  *       >按照连接的方式分类
  *         &连接
  *           两张变或者多张表结合在一起获取数据的过程
  *         &内连接
  *           #两张表进行连接查询，将两张表中完全匹配的记录查询出来
  *           #等值连接
  *           #非等值连接
  *           #自连接
  *         &外连接
  *           #两张表进行连接查询，将其中一张表的数据全部查询出来，另外一张表肯定有数据无法与其匹配，
  *             会自动模拟出空值进行匹配。
  *           #左(外)连接
  *           #右(外)连接
  *           #全连接
  *
  */
object Spark09_Transformation_join {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.1 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))

    //3.2 创建第二个pairRDD
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6),(2,8)))

    //join算子相当于内连接，将两个RDD中的key相同的数据匹配，如果key匹配不上，那么数据不关联
    //val newRDD: RDD[(Int, (String, Int))] = rdd.join(rdd1)
    //val newRDD: RDD[(Int, (Int, String))] = rdd1.join(rdd)

    //val newRDD: RDD[(Int, (String, Option[Int]))] = rdd.leftOuterJoin(rdd1)


    //cogroup
    val newRDD: RDD[(Int, (Iterable[String], Iterable[Int]))] = rdd.cogroup(rdd1)


    newRDD.collect().foreach(println)



    // 关闭连接
    sc.stop()
  }
}
