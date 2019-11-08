package com.fanhq.example.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author fanhaiqiu
 * @date 2019/11/8
 */
public class SparkSqlDemo6 {

    public static void main(String[] args) throws Exception{
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSqlDemo6")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read().json("data/person");
        df.select("name").write().format("json").save("data/out/demo6");
    }
}
