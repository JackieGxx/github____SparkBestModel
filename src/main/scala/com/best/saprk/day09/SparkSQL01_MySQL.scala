package com.best.saprk.day09

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * Author: Felix
  * Date: 2020/5/19
  * Desc: 通过jdbc对MySQL进行读写操作
  */
object SparkSQL01_MySQL {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_MySQL")
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    /*
    //从MySQL数据库中读取数据  方式1
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop202:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .load()
    df.show()

    //从MySQL数据库中读取数据  方式2
    val df: DataFrame = spark.read.format("jdbc")
      .options(
        Map(
          "url" -> "jdbc:mysql://hadoop202:3306/test?user=root&password=123456",
          "driver" -> "com.mysql.jdbc.Driver",
          "dbtable" -> "user"
        )
      ).load()
    df.show()

    //从MySQL数据库中读取数据  方式3
    val props: Properties = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")
    props.setProperty("driver","com.mysql.jdbc.Driver")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://hadoop202:3306/test","user",props)
    df.show()

    */

    //向MySQL数据库中写入数据
    val rdd: RDD[User] = spark.sparkContext.makeRDD(List(User("banzhang",20),User("jingjing",18)))
    //将RDD转换为DF
    //val df: DataFrame = rdd.toDF()
    //df.write.format("jdbc").save()

    //将RDD转换为DS
    val ds: Dataset[User] = rdd.toDS()
    ds.write.format("jdbc")
      .option("url", "jdbc:mysql://hadoop202:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option(JDBCOptions.JDBC_TABLE_NAME, "user")
      .mode(SaveMode.Append)
      .save()

    //释放资源
    spark.stop()
  }
}

case class User(name:String,age:Int)
