package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class ParquetDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ParquetDemo")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame userDF = sqlContext.read().parquet("D://input//users.parquet");
        userDF.show();
    }
}
