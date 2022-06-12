package com.best.saprk.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc:  通过读取外部文件，创建RDD
  *   -默认分区规则
  *       math.min(分配给应用的CPU核数,2)
  *   -指定分区
  *     >1.在textFile方法中，第二个参数minPartitions,表示最小分区数
  *       注意：是最小，不是实际的分区个数
  *     >2.在实际计算分区个数的时候，会根据文件的总大小和最小分区数进行相除运算
  *       &如果余数为0
  *         那么最小分区数，就是最终实际的分区数
  *       &如果余数不为0
  *         那么实际的分区数，要计算
  *
  *   -原始数据
  *       0 	1 	2 	3 	4 	5   6 	7 	8
  *       a	b 	c	d 	e 	f 	g 	X	X
  *
  *       9 	10 	11	12 	13	14
  *       h 	i 	j 	k	X 	X
  *
  *       15 	16 	17 	18	19
  *       l 	m 	n 	X 	X
  *
  *       20 	21
  *       o 	p
  *
  *       设置最小切片数为3
  *
  *
  *       切片规划 FileInputFormat->getSplits
  *       0 = {FileSplit@5103} "file:/D:/dev/workspace/bigdata-0105/spark-0105/input/test.txt:0+7"
  *       1 = {FileSplit@5141} "file:/D:/dev/workspace/bigdata-0105/spark-0105/input/test.txt:7+7"
  *       2 = {FileSplit@5181} "file:/D:/dev/workspace/bigdata-0105/spark-0105/input/test.txt:14+7"
  *       3 = {FileSplit@5237} "file:/D:/dev/workspace/bigdata-0105/spark-0105/input/test.txt:21+1"
  *
  *
  *       最终分区数据
  *       0
  *       abcdefg
  *       1
  *       hijk
  *       2
  *       lmn
  *       op
  *       3
  *       空
  *
  *
  */
object Spark05_Partition_file {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //从本地文件中读取数据，创建RDD

    //输入数据 1换行2换行3换行4 ,采用默认分区方式， 最终分区数2， 0->1,2    1->3,4
    //val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\input\\2.txt")

    //输入数据 1换行2换行3换行4 ,minPartitions设置为3， 最终分区数4， 0->1,2    1->3,  2->4,   3->
    //val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\input\\2.txt",3)

    //输入数据 123456 ,minPartitions设置为3， 最终分区数3， 0->123456
    //val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\input\\2.txt",3)

    //输入数据 123换行4567 ,minPartitions设置为3， 最终分区数3， 0->123，1->4,5,6,7
    //val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\input\\2.txt",3)


    //val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\input\\test.txt",3)

    val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\input\\3.txt",5)

    rdd.saveAsTextFile("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\output")

    // 关闭连接
    sc.stop()
  }
}
