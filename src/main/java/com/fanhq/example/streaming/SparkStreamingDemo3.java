package com.fanhq.example.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author fanhaiqiu
 * @date 2019/11/12
 */
public class SparkStreamingDemo3 {


    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingDemo3");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.19.3.194:9092,10.19.3.195:9092,10.19.3.196:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark-stream-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("intelligence-building");
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        JavaPairDStream<String, String> dStream = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        dStream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
            @Override
            public void call(JavaPairRDD<String, String> javaPairRDD) throws Exception {
                javaPairRDD.foreach((x -> {
                    System.out.println(x._1());
                    System.out.println(x._2());
                }));
            }
        });
        ssc.start();
        ssc.awaitTermination();
        ssc.close();
    }

}
