package cn.spark.study.sql;

import cn.spark.study.domain.Student;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class RDD2DataFrameReflection {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("RDD2DataFrameReflection");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc.sc());
        JavaRDD<String> lines = sc.textFile("D:\\input\\students.txt");

        JavaRDD<Student> stuRDD = lines.map(new Function<String, Student>() {
            private static final long serialVersionUID = -3263773318533214526L;

            @Override
            public Student call(String s) throws Exception {
                String [] linesSplit = s.split(",");
                Student stu = new Student();
                stu.setId(Integer.valueOf(linesSplit[0]));
                stu.setName(String.valueOf(linesSplit[1]));
                stu.setAge(Integer.valueOf(linesSplit[2]));
                return stu;
            }
        });

        DataFrame stuDataFrame = sqlContext.createDataFrame(stuRDD,Student.class);
        stuDataFrame.registerTempTable("students");
        DataFrame  dataFrame=sqlContext.sql("select * from students where age <=18");
        dataFrame.show();

        JavaRDD<Row> teenagerRDD = dataFrame.javaRDD();

        JavaRDD<Student> teenagerStudentRDD = teenagerRDD.map(new Function<Row, Student>() {
            private static final long serialVersionUID = -9073074202837320202L;

            @Override
            public Student call(Row row) throws Exception {
                Student stu = new Student();
                stu.setAge(row.getInt(0));
                stu.setId(row.getInt(1));
                stu.setName(row.getString(2));
                return stu;
            }
        });

        List<Student> list = teenagerStudentRDD.collect();
        for (Student stu:list){
            System.out.println(stu);
        }

    }
}
