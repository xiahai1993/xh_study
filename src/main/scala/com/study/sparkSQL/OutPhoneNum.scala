package com.study.sparkSQL

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object OutPhoneNum {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OutPhoneNum")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val linesDF = sqlContext.read.parquet("/DOMAIN_B/POSITION_BIG_DATA/tourism/data/cell-stay/hour="+args(0)+"*")

    val wdhd = sc.textFile("/xyf/shhd.txt").map(l=>(l.split(",")(0),l.split(",")(1))).filter(_._2!="上海").collectAsMap()

    val wdhdBro = sc.broadcast(wdhd)

    val phone = linesDF.map(l=>(l.getAs[String]("phoneNum"),l.getAs[String]("hour")+","+l.getAs("duration"))).filter(_._1.length==11)

    val phoneNum = phone.map(l=>(l._1.substring(0,7),l._1+","+l._2.split(",")(0).substring(0,8)+","+l._2.split(",")(1)))

    val phoneNumFilter = phoneNum.filter(t => wdhdBro.value.contains(t._1)).cache()

    val durSumByDay = phoneNumFilter.map(l=>(l._2.split(",")(0)+","+l._2.split(",")(1),l._2.split(",")(2).toInt)).reduceByKey(_+_)

    val fildpxqDur = durSumByDay.filter(_._2>=7200000)

    fildpxqDur.map(_._1).coalesce(1, true).saveAsTextFile("/xyf/wdPhoneNumFil/date="+args(0))

  }
}
