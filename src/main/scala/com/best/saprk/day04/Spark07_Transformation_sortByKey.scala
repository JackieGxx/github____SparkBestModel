package com.best.saprk.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/5/9
  * Desc: 转换算子-sortByKey
  *   -按照key对RDD中的 元素进行排序
  *
  */
object Spark07_Transformation_sortByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    /*
    //创建RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

    //按照key对rdd中的元素进行排序  默认升序
    //val newRDD: RDD[(Int, String)] = rdd.sortByKey()
    //降序
    val newRDD: RDD[(Int, String)] = rdd.sortByKey(false)

    newRDD.collect().foreach(println)
    */
    //如果key为自定义类型，要求必须混入Ordered特质

    val stdList: List[(Student, Int)] = List(
      (new Student("jingjing", 18), 1),
      (new Student("bangzhang", 18), 1),
      (new Student("jingjing", 19), 1),
      (new Student("luoxiang", 18), 1),
      (new Student("jingjing", 20), 1)
    )
    val stdRDD: RDD[(Student, Int)] = sc.makeRDD(stdList)

    val resRDD: RDD[(Student, Int)] = stdRDD.sortByKey()
    resRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}

class Student(var name:String,var age:Int) extends Ordered[Student] with Serializable {
  //指定比较规则
  override def compare(that: Student): Int = {
    //先按照名称排序升序，如果名称相同的话，再按照年龄降序排序
    var res: Int = this.name.compareTo(that.name)
    if(res == 0){
      res = that.age - this.age
    }
    res
  }

  override def toString = s"Student($name, $age)"
}
