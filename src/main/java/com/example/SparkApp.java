package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;

import java.util.Arrays;
import java.util.List;

public class SparkApp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark Example")
                .master("local[*]")
                .getOrCreate();

        List<String> data = Arrays.asList("apple", "banana", "orange", "grape");
        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        Dataset<Row> result = ds.toDF("fruit")
                                .filter("fruit LIKE 'a%'");

        result.show();
        SparkConf conf = new SparkConf().setAppName("Java Spark Example").setMaster("local");

        spark.stop();
    }
}

