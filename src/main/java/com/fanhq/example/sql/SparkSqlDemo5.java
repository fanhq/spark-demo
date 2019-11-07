package com.fanhq.example.sql;

import com.fanhq.example.common.Person;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author fanhaiqiu
 * @date 2019/11/7
 */
public class SparkSqlDemo5 {

    public static void main(String[] args) throws Exception{
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSqlDemo5")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read().json("data/person");
        df.createOrReplaceTempView("person");
        Dataset<Row> sqlDF = spark.sql("SELECT name,age,sex FROM person");

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("sex", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);

        List<Person> persons = spark.createDataFrame(sqlDF.toJavaRDD(), schema)
                .as(Encoders.bean(Person.class))
                .collectAsList();
        System.out.println(persons.size());

    }
}
