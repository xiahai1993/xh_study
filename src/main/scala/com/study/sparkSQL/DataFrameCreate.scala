package com.study.sparkSQL

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameCreate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("DataFrameCreate")
    val sc = new SparkContext()
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("")
    df.show()
  }
}
