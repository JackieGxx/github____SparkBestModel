package com.best.saprk.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-partitionBy
  *   对KV类型的RDD按照key进行重新分区
  *
  */
object Spark01_Transformation_partitionBy {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    //注意：RDD本身是没有partitionBy这个算子的，通过隐式转换动态给kv类型的RDD扩展的功能
    //val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    //rdd.partitionBy()
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1,"aaa"),(2,"bbb"),(3,"ccc")),3)

    rdd.mapPartitionsWithIndex{
      (index,datas)=>{
        println(index + "------" + datas.mkString(","))
        datas
      }
    }.collect()

    println("----------------------------------------------")

    //val newRDD: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))
    val newRDD: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(2))
    newRDD.mapPartitionsWithIndex{
      (index,datas)=>{
        println(index + "------" + datas.mkString(","))
        datas
      }
    }.collect()

    // 关闭连接
    sc.stop()
  }
}

//自定义分区器
class MyPartitioner(partitions: Int)extends Partitioner{

  //获取分区个数
  override def numPartitions: Int = partitions

  //指定分区规则   返回值Int表示分区编号，从0开始
  override def getPartition(key: Any): Int = {
    //1
    //8
    //val key: String = key.asInstanceOf[String]
    //if(key.startsWith("135")){
    //  0
    //}else{
    //  1
    //}
    1
  }
}
