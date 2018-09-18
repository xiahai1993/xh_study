package cn.spark.study.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class UpdateStateByKeyWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("UpdateStateByKeyWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 第一点，如果要使用updateStateByKey算子，就必须设置一个checkpoint目录，开启checkpoint机制
        // 这样的话才能把每个key对应的state除了在内存中有，那么是不是也要checkpoint一份
        // 因为你要长期保存一份key的state的话，那么spark streaming是要求必须用checkpoint的，以便于在
        // 内存数据丢失的时候，可以从checkpoint中恢复数据

        // 开启checkpoint机制，很简单，只要调用jssc的checkpoint()方法，设置一个hdfs目录即可
        jssc.checkpoint("");

        //然后实现wordcount逻辑
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }

        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(

                new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String word)
                            throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }

                });
        JavaPairDStream<String,Integer> wordCount = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            private static final long serialVersionUID = -806861340674349247L;

            // 这里两个参数
            // 实际上，对于每个单词，每次batch计算的时候，都会调用这个函数
            // 第一个参数，values，相当于是这个batch中，这个key的新的值，可能有多个吧
            // 比如说一个hello，可能有2个1，(hello, 1) (hello, 1)，那么传入的是(1,1)
            // 第二个参数，就是指的是这个key之前的状态，state，其中泛型的类型是你自己指定的
            @Override
            public Optional<Integer> call(List<Integer> integers, Optional<Integer> integerOptional) throws Exception {
                Integer newValue =0;

                if(integerOptional.isPresent()){
                    newValue=integerOptional.get();
                }

                for (Integer values:integers){
                    newValue+=values;
                }


                return Optional.of(newValue);
            }
        });


        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
