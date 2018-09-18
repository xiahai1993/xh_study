package com.study.sparkSQL

import org.apache.spark.{SparkConf, SparkContext}

object CountDayByWdPhoneNum {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CountDayByWdPhoneNum")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/xyf/wdPhoneNumFil/*/part-00000")
    lines
      .map(l=>(l.split(",")(0),1))
      .reduceByKey(_+_)
      .filter(_._2>=args(1).toInt)
      .coalesce(1,true)
      .saveAsTextFile("/xyf/wdphoneNum/date="+args(0))
  }
}
