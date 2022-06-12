package com.best.saprk.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/18
  * Desc: 求平均年龄----通过累加器实现
  */
object SparkSQL04_Accumulator {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建一个RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("jingjing",20),("bangzhang",40),("xuchong",30)))

    //创建累加器对象
    val myAcc = new MyAccumulator

    //注册累加器
    sc.register(myAcc)

    //使用累加器
    rdd.foreach{
      case (name,age)=>{
        myAcc.add(age)
      }
    }

    //获取累加器的值
    println(myAcc.value)


    // 关闭连接
    sc.stop()
  }
}

class MyAccumulator extends AccumulatorV2[Int,Double]{
  var ageSum:Int = 0
  var countSum:Int = 0

  override def isZero: Boolean = {
    ageSum == 0 && countSum ==0
  }

  override def copy(): AccumulatorV2[Int, Double] = {
    var newMyAcc = new MyAccumulator
    newMyAcc.ageSum = this.ageSum
    newMyAcc.countSum = this.countSum
    newMyAcc
  }

  override def reset(): Unit = {
    ageSum = 0
    countSum = 0
  }

  override def add(age: Int): Unit = {
    ageSum += age
    countSum += 1
  }

  override def merge(other: AccumulatorV2[Int, Double]): Unit = {
    other match {
      case mc: MyAccumulator=>{
        this.ageSum += mc.ageSum
        this.countSum += mc.countSum
      }
      case _ =>
    }
  }

  override def value: Double = {
    ageSum/countSum
  }
}