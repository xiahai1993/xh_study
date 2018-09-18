package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class ParquetLoadData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ParquetLoadData")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame usersDF = sqlContext.read().parquet("D://input//users.parquet");
        usersDF.registerTempTable("users");
        DataFrame usersNameDF = sqlContext.sql("select name from users");
        JavaRDD<String> usersNameRDD = usersNameDF.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        });

        usersNameRDD.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = 5138803501743152599L;

            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }
}
