package com.study.sparkSQL

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object RDD2DataFrameProgrammatically extends App {
    val conf =new SparkConf()
    conf.setAppName("RDD2DataFrameProgrammatically")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val lines = sc.textFile("D://input//students.txt")
    //第一步：将RDD转换为row
    val linesRow = lines.map(line=>Row(line.split(",")(0).toInt,line.split(",")(1),line.split(",")(2).toInt))
    //第二步：动态创建元数据
    val structType = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))
    //第三步：转换为DataFrame
    val studentDF = sQLContext.createDataFrame(linesRow,structType)
    studentDF.registerTempTable("students")
  val teenagerDf = studentDF.sqlContext.sql("select*from students where age<=18")
  teenagerDf.show()
}