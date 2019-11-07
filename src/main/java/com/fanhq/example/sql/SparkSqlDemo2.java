package com.fanhq.example.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author fanhaiqiu
 * @date 2019/11/7
 */
public class SparkSqlDemo2 {

    public static void main(String[] args) {
        SparkSession session = SparkSession
                .builder()
                .appName("SparkSqlDemo2")
                .master("local")
                .getOrCreate();
        SQLContext sqlContext = new SQLContext(session);
        Dataset<Row> dataset = sqlContext.read().json("data/person");
        dataset.show();
        dataset.printSchema();
    }
}
