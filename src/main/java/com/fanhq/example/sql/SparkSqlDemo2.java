package com.fanhq.example.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * @author fanhaiqiu
 * @date 2019/11/7
 */
public class SparkSqlDemo2 {

    public static void main(String[] args) throws Exception{
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSqlDemo2")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read().json("data/person");
        df.show();
        df.printSchema();
        df.select("name").show();
        df.select(col("name"), col("age").plus(1)).show();
        df.filter(col("age").gt(21)).show();
        df.groupBy("age").count().show();

    }
}
