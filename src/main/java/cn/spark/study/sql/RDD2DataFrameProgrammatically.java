package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.List;

public class RDD2DataFrameProgrammatically {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("RDD2DataFrameProgrammatically");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc.sc());
        JavaRDD<String> lines = sc.textFile("D://input//students.txt");

        //第一步：将RDD转换为Row
        JavaRDD<Row> linesRow = lines.map(new Function<String, Row>() {
            private static final long serialVersionUID = -5867704421532683935L;

            @Override
            public Row call(String line) throws Exception {
                String [] arr = line.split(",");

                return RowFactory.create
                        (Integer.valueOf(arr[0]),
                        String.valueOf(arr[1]),
                        Integer.valueOf(arr[2]));
            }
        });
        //第二步：动态创建元数据
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(structFields);
        //第三步：转换为DataFrame
        DataFrame studentDF = sqlContext.createDataFrame(linesRow,structType);
        //第四步：创建临时表
        studentDF.registerTempTable("students");
        DataFrame teenagersDF = studentDF.sqlContext().sql("select*from students where age<=18");

        teenagersDF.show();

        sc.close();
    }
}
