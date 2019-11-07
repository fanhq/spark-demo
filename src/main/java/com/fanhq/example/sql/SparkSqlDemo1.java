package com.fanhq.example.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author fanhaiqiu
 * @date 2019/11/7
 */
public class SparkSqlDemo1 {

    public static void main(String[] args) {
        //初始化SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSqlDemo1")
                .master("local")
                .getOrCreate();
        //读取元数据文件
        Dataset<Row> df = spark.read().json("data/person");
        //生成rdd
        JavaRDD<Row> rdd = df.toJavaRDD();
        //遍历
        rdd.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row.toString());
            }
        });

        spark.stop();
    }
}
