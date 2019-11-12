package com.fanhq.example.streaming;

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
import java.util.Iterator;

/**
 * @author fanhaiqiu
 * @date 2019/11/11
 */
public class SparkStreamingDemo1 {

    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkStreamingDemo1");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(3));
        JavaDStream<String> dStream = jsc.textFileStream("data/in");
        JavaDStream<String> words = dStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                System.out.println(s);
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        //计算每个单词出现的个数
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        //输出结果
        wordCounts.print();

        //开始作业
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
