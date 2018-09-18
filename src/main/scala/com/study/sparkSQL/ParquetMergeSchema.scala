package com.study.sparkSQL

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object ParquetMergeSchema {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ParquetMergeSchema")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // 创建一个DataFrame，作为学生的基本信息，并写入一个parquet文件中
    val studentsWithNameAge = Seq(("zhangsan",24),("lisi","18"))
    val studentsWithNameAgeDF = sc.parallelize(studentsWithNameAge,2).toDF("name","age")
    studentsWithNameAgeDF.save("","parquet",SaveMode.Append)
    // 创建二个DataFrame，作为学生的成绩信息，并写入一个parquet文件中
    val studentsWithNameGrade = Seq(("zhangsan",80),("wangwu",60))
    val studentsWithNameGradeDF = sc.parallelize(studentsWithNameGrade,2).toDF("name","grade")
   studentsWithNameGradeDF.save("","parquet",SaveMode.Append)
    // 首先，第一个DataFrame和第二个DataFrame的元数据肯定是不一样的吧
    // 一个是包含了name和age两个列，一个是包含了name和grade两个列
    // 所以， 这里期望的是，读取出来的表数据，自动合并两个文件的元数据，出现三个列，name、age、grade

    // 用mergeSchema的方式，读取students表中的数据，进行元数据的合并
    val students = sqlContext.read.option("mergeSchema","true").parquet("")
    students.printSchema()
    students.show()


  }
}
