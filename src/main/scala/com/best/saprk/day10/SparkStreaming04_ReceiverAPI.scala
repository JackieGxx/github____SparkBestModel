package com.best.saprk.day10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: Felix
  * Date: 2020/5/20
  * Desc:   通过ReceiverAPI连接Kafka数据源，获取数据
  */
object SparkStreaming04_ReceiverAPI {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming02_RDDQueue").setMaster("local[*]")

    //创建SparkStreaming上下文环境对象
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //连接Kafka，创建DStream
    val kafkaDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "hadoop202:2181,hadoop203:2181,hadoop204:2181",
      "bigdata",
      Map("bigdata0105" -> 2)
    )
    //获取kafka中的消息，我们只需要v的部分
    val lineDS: DStream[String] = kafkaDstream.map(_._2)

    //扁平化
    val flatMapDS: DStream[String] = lineDS.flatMap(_.split(" "))

    //结构转换  进行计数
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_,1))

    //聚合
    val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)

    //打印输出
    reduceDS.print

    //开启任务
    ssc.start()

    ssc.awaitTermination()
  }
}
