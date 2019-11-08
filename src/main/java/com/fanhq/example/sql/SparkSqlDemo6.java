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
        Dataset<Row> df1 = spark.read().json("data/person");
        df1.select("name").write().format("json").save("data/out/demo6");

        Dataset<Row> df2 = spark.read().format("json").load("data/out/demo6");
        df2.show();
    }
}
