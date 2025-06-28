package com.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class SparkApp {
    public static void main(String[] args) {
//        SparkSession spark = SparkSession.builder()
//                .appName("Java Spark Example")
//                .master("local[*]")
//                .getOrCreate();
//
//        List<String> data = Arrays.asList("apple", "banana", "orange", "grape");
//        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
//
//        Dataset<Row> result = ds.toDF("fruit")
//                .filter("fruit LIKE 'a%'");
//
//        result.show();
//
//        // ✅ Get existing SparkContext from SparkSession
//        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//
//        JavaRDD<String> dd = jsc.parallelize(data);
//        dd.foreach(x -> System.out.println("From RDD: " + x));
//
//        spark.close();

        List<Double> inputData=new ArrayList<>();
        inputData.add(1.0);
        inputData.add(2.0);
        inputData.add(3.0);
        inputData.add(4.0);


        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf=new SparkConf().setAppName("startingSpark").
                setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<Double> myRdd=sc.parallelize(inputData);
        JavaRDD<Double>d=myRdd.map((val)->val*2);

        JavaRDD<IntegerWIthDoubleNumber>dn=myRdd.map(val->new IntegerWIthDoubleNumber(val));
//        dn.collect().forEach(System.out::println);
        dn.collect().forEach(obj -> {
            System.out.println("inputData = " + obj.inputData + ", inputData2 = " + obj.inputData2);
        });
//        dn.foreach(x->System.out.println(x));
        /*
        /tuples use to group multiple set of rdd
        in tuples multip-le instance of the same key possible
         */
        Tuple2<Double, Double> tuple2= new Tuple2<>(1.0,2.0);
        JavaRDD<Tuple2<Double,Double>>ftnum=myRdd.map(val->new Tuple2<>(val,val*4));
        JavaRDD<Map<Double, Double>> resultRdd = myRdd.zip(d).map(pair -> Collections.singletonMap(pair._1, pair._2));
        ftnum.collect().forEach(obj -> {
            System.out.println(obj._1);
            System.out.println("four times ");
            System.out.println(obj._2);
        });
        // Triggering an action (optional)
//        resultRdd.foreach(x->System.out.println(x));
//        JavaRDD<Integer> fd=myRdd.map(val->1);
//        long sz=fd.reduce((val1,val2)->val1+val2);
//        System.out.println("kkknn");
//        System.out.println(sz);
//        System.out.println("kkknn");
//        Double res=myRdd.reduce((value1,value2)->value1+value2);
//        Double r=myRdd.reduce(Double::sum);
//
//        System.out.println(myRdd.collect());
//        System.out.println(res);
//        System.out.println(r);
//        System.out.println(d.collect());
//        d.collect().forEach(System.out::println);
//        d.foreach(val-> System.out.println(val));
//        d.count();


        sc.close();
    }
}
