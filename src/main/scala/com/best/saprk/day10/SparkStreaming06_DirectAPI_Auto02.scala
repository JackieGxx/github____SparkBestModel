package com.best.saprk.day10

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: Felix
  * Date: 2020/5/20
  * Desc:   通过DirectAPI连接Kafka数据源，获取数据
  *   自定的维护偏移量，偏移量维护在checkpiont中
  *   修改StreamingContext对象的获取方式，先从检查点获取，如果检查点没有，通过函数创建。会保证数据不丢失
  * 缺点：
  *   1.小文件过多
  *   2.在checkpoint中，只记录最后offset的时间戳，再次启动程序的时候，会从这个时间到当前时间，把所有周期都执行一次
  */
object SparkStreaming06_DirectAPI_Auto02 {
  def main(args: Array[String]): Unit = {
    //创建Streaming上下文环境对象
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\cp",()=>getStreamingContext)

    ssc.start()
    ssc.awaitTermination()

  }

  def getStreamingContext():StreamingContext ={
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming02_RDDQueue").setMaster("local[*]")

    //创建SparkStreaming上下文环境对象
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //设置检查点目录
    ssc.checkpoint("D:\\dev\\workspace\\bigdata-0105\\spark-0105\\cp")

    //准备Kafka参数
    val kafkaParams: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop202:9092,hadoop203:9092,hadoop204:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata"
    )


    val kafkaDstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set("bigdata-0105")
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

    ssc
  }
}
