package com.fanhq.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author fanhaiqiu
 * @date 2019/9/17
 */
public class SparkApplication {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("appName").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("data.txt/data.txt");
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println(totalLength);
    }
}
