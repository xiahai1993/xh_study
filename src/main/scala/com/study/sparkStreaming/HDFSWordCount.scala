package com.study.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

object HDFSWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("HDFSWordCount")
    val ssc = new StreamingContext(conf,Durations.seconds(5))

    val lines = ssc.textFileStream("D://input//wordcount//word")

    val word = lines.flatMap(_.split(" "))

    val pairs = word.map(l=>(l,1))

    val wordCount = pairs.reduceByKey(_+_)

    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
