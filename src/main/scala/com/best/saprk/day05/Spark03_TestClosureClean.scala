package com.best.saprk.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/13
  * Desc: 闭包情况演示
  */
object Spark03_TestClosureClean {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf()
                            .setAppName("SparkCoreTest")
                            .setMaster("local[*]")
                            // 替换默认的序列化机制
                            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                            // 注册需要使用 kryo 序列化的自定义类
                            .registerKryoClasses(Array(classOf[Seacher]))


    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello","atguigui","hello Spark","jingjing"))

    //创建一个Searcher对象
    val seacher = new Seacher("hello")

    val newRDD: RDD[String] = seacher.getMatch1(rdd)

    newRDD.foreach(println)

    // 关闭连接
    sc.stop()
  }
}

//class Seacher(str:String) extends Serializable {

//class Seacher(str:String){

case class Seacher(str:String){

  //判断字符串中是否包含str子字符串
  //def isMatch(elem:String): Boolean ={
  //  elem.contains(str)
  //}

  //对RDD[String]中的元素进行过滤，过滤出包含str字符串的元素
  //def getMatch1(rdd:RDD[String]): RDD[String] ={
  //  //rdd.filter(isMatch)
  //  rdd.filter(this.isMatch)
  //}

  def getMatch1(rdd:RDD[String]): RDD[String] ={
    //rdd.filter(elem=>elem.contains(str))
    rdd.filter(elem=>elem.contains(this.str))
    //var ss:String = this.str
    //rdd.filter(elem=>elem.contains(ss))


  }

}