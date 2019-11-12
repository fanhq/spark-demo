package com.fanhq.example.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author fanhaiqiu
 * @date 2019/11/7
 */
public class SparkSqlDemo3 {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSqlDemo3")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> df = spark.read().json("data/person");
        df.createOrReplaceTempView("person");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM person");
        sqlDF.show();
    }
}
