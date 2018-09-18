package com.study.sparkSQL

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HiveDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HiveDataSource").setMaster("local")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    hiveContext.sql("DROP TABLE IF EXISTS student_infos")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING,age INT)")
    hiveContext.sql("LOAD DATA " +
      "LOCAL INPATH 'xxx.txt'" +
      "INTO TABEL student_infos")

    hiveContext.sql("DROP TABLE IF EXISTS student_scores");
    hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT)");
    hiveContext.sql("LOAD DATA "
      + "LOCAL INPATH '/usr/local/spark-study/resources/student_scores.txt' "
      + "INTO TABLE student_scores")

    val goodStudentsInfo = hiveContext.sql("SELECT si.name,si.age,ss.score" +
      "FROM student_infos si " +
      "JOIN student_scores ss ON si.name = ss.name" +
      "WHERE ss.score>=80")

    hiveContext.sql("DROP TABLE IF NOT EXISTS goodStudents_info")
    goodStudentsInfo.saveAsTable("goodStudents_info")

    for(goodStudentRow <- goodStudentsInfo) {
      println(goodStudentRow);
    }

  }
}
