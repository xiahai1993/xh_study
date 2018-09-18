package com.study.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val ssc = new StreamingContext(conf,Seconds(1))
    val lines = ssc.socketTextStream("localhost",9999)
    val words = lines.flatMap(_.split(","))
    val pairs = words.map(l=>(l,1))
    val wordCount = pairs.reduceByKey(_+_)

    Thread.sleep(5000)
    wordCount.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
