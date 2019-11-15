package com.fanhq.example.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.util.HashMap;
import java.util.Map;

/**
 * @author fanhaiqiu
 * @date 2019/11/15
 */
public class SparkStreamingDemo4 {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingDemo4");
        SparkContext sparkContext = new SparkContext(conf);
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.19.3.194:9092,10.19.3.195:9092,10.19.3.196:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark-stream-group");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", true);


        OffsetRange[] offsetRanges = {
                // topic, partition, inclusive starting offset, exclusive ending offset
                OffsetRange.create("intelligence-building", 0, 0, 100),
                OffsetRange.create("intelligence-building", 1, 0, 100),
                OffsetRange.create("intelligence-building", 2, 0, 100),
        };

        RDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(
                sparkContext,
                kafkaParams,
                offsetRanges,
                LocationStrategies.PreferConsistent()
        );
        rdd.toJavaRDD().foreach(new VoidFunction<ConsumerRecord<String, String>>() {
            @Override
            public void call(ConsumerRecord<String, String> record) throws Exception {
                System.out.println(record.value());
            }
        });
    }
}
