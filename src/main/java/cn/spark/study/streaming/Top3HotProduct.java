package cn.spark.study.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 与SparkSQL整合，TOP3热门商品统计
 */
public class Top3HotProduct {
    public static void main(String[] args) {
        //创建SparkStreaming上下文
        SparkConf conf =new SparkConf()
                .setMaster("local[2]")
                .setAppName("Top3HotProduct");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // 获取输入数据流（数据格式：leo iphone mobile_phone）
        JavaReceiverInputDStream<String> productCategoryDS = jssc.socketTextStream("spark1",9999);
        //做一个映射，将每个种类的每个商品，映射为(category_product, 1)
        JavaPairDStream<String,Integer> pairDStream = productCategoryDS.mapToPair(
                new PairFunction<String, String, Integer>() {
                    private static final long serialVersionUID = 5935096426419881036L;

                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s.split(" ")[2]
                                +"_"+s.split(" ")[1],1);
                    }
                }
        );
        //执行window操作
        JavaPairDStream<String,Integer> category_productWordCount = pairDStream.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = -2694633884061803195L;
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                },Durations.seconds(60),Durations.seconds(10)
        );

        // 针对60秒内的每个种类的每个商品的点击次数
        // foreachRDD，在内部，使用Spark SQL执行top3热门商品的统计
        category_productWordCount.foreachRDD(
                new Function<JavaPairRDD<String, Integer>, Void>() {
                    private static final long serialVersionUID = -6722969340438925891L;

                    @Override
                    public Void call(JavaPairRDD<String, Integer> pairRDD) throws Exception {
                        // 将该RDD，转换为JavaRDD<Row>的格式
                        JavaRDD<Row> categoryProductCountsRow = pairRDD.map(
                                new Function<Tuple2<String, Integer>, Row>() {
                                    private static final long serialVersionUID = 8811359216676754788L;

                                    @Override
                                    public Row call(Tuple2<String, Integer> tuple2) throws Exception {
                                        return RowFactory.create(
                                                tuple2._1.split("_")[0],
                                                tuple2._1.split("_")[1],
                                                tuple2._2);
                                    }
                                }
                        );

                        // 然后，执行DataFrame转换
                        List<StructField> structFields = new ArrayList<StructField>();
                        structFields.add(DataTypes.createStructField("category",DataTypes.StringType,true));
                        structFields.add(DataTypes.createStructField("product",DataTypes.StringType,true));
                        structFields.add(DataTypes.createStructField("click_count",DataTypes.StringType,true));
                        StructType structType = DataTypes.createStructType(structFields);

                        HiveContext hiveContext = new HiveContext(categoryProductCountsRow.context());
                        // 将60秒内的每个种类的每个商品的点击次数的数据，注册为一个临时表
                        DataFrame categoryProductClickDF = hiveContext.createDataFrame(categoryProductCountsRow,structType);

                        categoryProductClickDF.registerTempTable("product_click_log");

                        // 执行SQL语句，针对临时表，统计出来每个种类下，点击次数排名前3的热门商品
                        DataFrame productTop3DF = hiveContext.sql(
                                "SELECT category,product,click_count "
                                        + "FROM ("
                                            + "SELECT "
                                                + "category,"
                                                + "product,"
                                                + "click_count,"
                                                + "row_number() OVER (PARTITION BY category ORDER BY click_count DESC) rank "
                                            + "FROM product_click_log"
                                        + ") tmp "
                                        + "WHERE rank<=3");
                        productTop3DF.show();



                        return null;
                    }
                }
        );


        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }

}
