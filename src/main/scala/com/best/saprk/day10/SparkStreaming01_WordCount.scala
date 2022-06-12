package com.best.saprk.day10

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * Author: Felix
  * Date: 2020/5/20
  * Desc: 
  */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象   注意：Streaming程序执行至少需要2个线程，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming01_WordCount").setMaster("local[*]")

    //创建SparkStreaming程序执行入口对象（上下文环境对象）
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //从指定的端口获取数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop202",9999)

    //扁平化
    val flatMapDS: DStream[String] = socketDS.flatMap(_.split(" "))
    
    //结构转换  进行计数
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_,1))

    //聚合
    val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)



    //打印输出
    reduceDS.print

    //启动采集器
    ssc.start()

    //默认情况下，采集器不能关闭
    //ssc.stop()


    //等待采集结束之后，终止程序
    ssc.awaitTermination()
  }
}
