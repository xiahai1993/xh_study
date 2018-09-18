package com.study.sparkSQL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object ParquetLoadData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetLoadData").setMaster("local")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val users = sQLContext.read.parquet("")
    users.registerTempTable("users")

    val usersDF = sQLContext.sql("select * from users")


  }
}
