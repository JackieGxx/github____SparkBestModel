package com.best.saprk.day10

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: Felix
  * Date: 2020/5/20
  * Desc:   通过DirectAPI连接Kafka数据源，获取数据
  *       手动维护offset
  */
object SparkStreaming07_DirectAPI_Hander {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming07_DirectAPI_Hander").setMaster("local[*]")

    //创建SparkStreaming上下文环境对象
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //准备Kafka参数
    val kafkaParams: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop202:9092,hadoop203:9092,hadoop204:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata"
    )

    //获取上一次消费的位置（偏移量）
    //实际项目中，为了保证数据精准一致性，我们对数据进行消费处理之后，将偏移量保存在有事务的存储中， 如MySQL
     var  fromOffsets:Map[TopicAndPartition,Long] = Map[TopicAndPartition,Long](
        TopicAndPartition("bigdata-0105",0)->10L,
        TopicAndPartition("bigdata-0105",1)->10L
      )

    //从指定的offset读取数据进行消费
    val kafkaDstream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc,
      kafkaParams,
      fromOffsets,
      (m: MessageAndMetadata[String, String]) => m.message()
    )

    //消费完毕之后，对偏移量offset进行更新
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    kafkaDstream.transform{
      rdd=>{
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }.foreachRDD{
      rdd=>{
        for (o <- offsetRanges) {
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }


}
