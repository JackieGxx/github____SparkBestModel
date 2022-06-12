package com.best.saprk.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-sample
  *   随机抽样
  */
object Spark08_Transformation_sample {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    //val rdd: RDD[Int] = sc.makeRDD(1 to 10)

    /*
      -withReplacement  是否抽样放回
        true    抽样放回
        false   抽样不放回
      -fraction
        withReplacement=true   表示期望每一个元素出现的次数  >0
        withReplacement=false  表示RDD中每一个元素出现的概率[0,1]
      -seed 抽样算法初始值
        一般不需要指定
    */
    //从rdd中随机抽取一些数据(抽样放回)
    //val newRDD: RDD[Int] = rdd.sample(true,3)


    //从rdd中随机抽取一些数据(抽样不放回)
    //val newRDD: RDD[Int] = rdd.sample(false,0.6)
    //newRDD.collect().foreach(println)


    val stds: List[String] = List("于秩晨", "李嘉腾", "谭雁成", "郑欣", "赵跃", "卢凯旋",
      "许耀", "郝俊杰", "闫晨", "刘野", "王子文", "赵玉冬",
      "刘振飞", "于雪峰", "王强", "韩琦", "李斌", "马中渊", "王天临", "石哲",
      "胡金泊", "徐斐", "李海峰", "王高鹏", "丁晨", "张浩然", "郭伟",
      "竹永亮", "张嘉峰", "廖保林", "罗翔",
      "夏瑞浩", "王振伟", "方旭翀", "张薄薄",
      "董佳琳", "刘俊芃", "赵煜", "毛兴达",
      "周奇巍", "马静", "刘阳", "刘昊",
      "何成江", "曹震", "李小龙", "郑玄毅")

    val nameRDD: RDD[String] = sc.makeRDD(stds)

    //从上面RDD中抽取一名幸运同学进行连麦

    val luckyMan: Array[String] = nameRDD.takeSample(false,1)

    luckyMan.foreach(println)


    // 关闭连接
    sc.stop()
  }
}
