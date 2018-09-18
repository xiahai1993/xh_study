package cn.spark.study.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class HDFSWordCount {
    public static void main(String[] args) {
        SparkConf conf  = new SparkConf()
                .setMaster("local[2]")
                .setAppName("HDFSWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));

        JavaDStream<String> lines = jssc.textFileStream("D://input//wordcount//word");

        JavaDStream<String> word = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = -1478628789119926243L;

            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        JavaPairDStream<String,Integer> pairs = word.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 3749572813208370240L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        });

        JavaPairDStream<String,Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 5042990373438056412L;

            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        wordCount.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
