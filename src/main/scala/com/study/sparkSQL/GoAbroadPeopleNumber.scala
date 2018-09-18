package com.study.sparkSQL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object GoAbroadPeopleNumber {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GoAbroadPeopleNumber")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val lines = sqlContext.read.parquet("/DOMAIN_B/POSITION_BIG_DATA/tourism/data/cell-stay")

    lines.registerTempTable("cell_stay")

    val phoneNumDF = sqlContext.sql("select * from cell_stay where hour between '"+ args(0) + "' and '"+ args(1) +"'")

    phoneNumDF.registerTempTable("timeLater_phoneNum")

    val lastTimePhoneNumDF = sqlContext.sql("select * from (select *,row_number() over(partition by phoneNum order by 'startTime' desc) as index from timeLater_phoneNum) as a where a.index=1").cache()

    val lacCellIdDF = lastTimePhoneNumDF.map(row => (row(4)+"-"+row(5),row(3).toString))

    val jc = sc.textFile("/xyf/jc.txt").map(l=>(l.split(",")(0),l.split(",")(1)))

    val JClacCellIdDF = lacCellIdDF.join(jc)

    val JCphoneNum = JClacCellIdDF.map(l=>(l._2._1,l._2._2)).cache()

    val linesNLA = sqlContext.read.parquet("/DOMAIN_B/POSITION_BIG_DATA/fraud-monitoring/data/non-local-appearance")

    linesNLA.registerTempTable("non_local_appearance")

    val phoneNumNLADF = sqlContext.sql("select phoneNum from non_local_appearance where hour between '"+ args(1) + "' and '"+ args(2) +"'")

    val NLAphoneNum = phoneNumNLADF.map(row=>row(0).toString).cache()

    val JCsubtractNLA = JCphoneNum.map(_._1).subtract(NLAphoneNum)

    val SHphoneNum = sqlContext.sql("select phoneNum from cell_stay where hour between '"+ args(1) + "' and '"+ args(2) +"'")

    val SHphoneNumRDD = SHphoneNum.map(row=>row(0).toString).cache()

    val subtractSH = JCsubtractNLA.subtract(SHphoneNumRDD)

    val shhd = sc.textFile("/xyf/shhd.txt").filter(_.split(",")(1)=="上海").map(l=>(l.split(",")(0),l.split(",")(1))).cache()

    val goAbroadPeopleNum = subtractSH.map(l=>(l.substring(0,7),l)).join(shhd)

    goAbroadPeopleNum.map(_._2._1).coalesce(1,true).saveAsTextFile("/xyf/goAbroadphoneNum/"+args(0))
  }
}
