package com.fanhq.example.sql;

import com.fanhq.example.common.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;

/**
 * @author fanhaiqiu
 * @date 2019/11/7
 */
public class SparkSqlDemo4 {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSqlDemo4")
                .master("local[*]")
                .getOrCreate();
        JavaRDD<Person> peopleRDD = spark.read()
                .json("data/person")
                .javaRDD()
                .map(line -> {
                    Person person = new Person();
                    person.setAge(line.getLong(0));
                    person.setName(line.getString(1));
                    person.setSex(line.getString(2));
                    return person;
                });
        peopleRDD.foreach((VoidFunction<Person>) person -> System.out.println(person.getName()));

        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        peopleDF.createOrReplaceTempView("person");

        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM person WHERE age BETWEEN 13 AND 19");

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                stringEncoder);
        teenagerNamesByIndexDF.show();

        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder);
        teenagerNamesByFieldDF.show();
    }
}
