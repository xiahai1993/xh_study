package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class SaveModeTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SaveModeTest")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame peopleDF = sqlContext.read().format("json")
                .load("D://input//people.json");
        peopleDF.show();
        peopleDF.save("D://input","json",SaveMode.Append);
    }
}
