package com.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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

//        List<Double> inputData=new ArrayList<>();
//        inputData.add(1.0);
//        inputData.add(2.0);
//        inputData.add(3.0);
//        inputData.add(4.0);


        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").
                setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<Double> myRdd=sc.parallelize(inputData);
//        JavaRDD<Double>d=myRdd.map((val)->val*2);

//        JavaRDD<IntegerWIthDoubleNumber>dn=myRdd.map(val->new IntegerWIthDoubleNumber(val));
////        dn.collect().forEach(System.out::println);
//        dn.collect().forEach(obj -> {
//            System.out.println("inputData = " + obj.inputData + ", inputData2 = " + obj.inputData2);
//        });
//        dn.foreach(x->System.out.println(x));
        /*
        /tuples use to group multiple set of rdd
        in tuples multip-le instance of the same key possible
         */
//        Tuple2<Double, Double> tuple2= new Tuple2<>(1.0,2.0);
//        JavaRDD<Tuple2<Double,Double>>ftnum=myRdd.map(val->new Tuple2<>(val,val*4));
//        JavaRDD<Map<Double, Double>> resultRdd = myRdd.zip(d).map(pair -> Collections.singletonMap(pair._1, pair._2));
//        ftnum.collect().forEach(obj -> {
//            System.out.println(obj._1);
//            System.out.println("four times ");
//            System.out.println(obj._2);
//        });
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


        /*
        / Pair RDD
         */
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: KEY 1 OF WARN ");
        inputData.add("WARN: KEY 2 OF WARN ");
        inputData.add("LEVEL: KEY 3 OF LOGGER ");
        inputData.add("INFO: KEY 1 OF WARN ");

        JavaRDD<String> data = sc.parallelize(inputData);
        JavaPairRDD<String, String> result = data.mapToPair(value -> {
            String columns[] = value.split(":");
            String logger = columns[0];
            String msg = columns[1];
            return new Tuple2<>(logger, msg);
        });

        // reducebyKey

        JavaPairRDD<String, Integer> result2 = data.mapToPair(value -> {
            String columns[] = value.split(":");
            String logger = columns[0];
            String msg = columns[1];
            return new Tuple2<>(logger, 10);
        });
        JavaPairRDD<String, Integer> ans = result2.reduceByKey((a, b) -> a + b);
//        ans.collect().forEach(tuple -> {
//            System.out.println(tuple._1 + " has " + tuple._2 + " value ");
//        });
//        result.collect().forEach(System.out::println);

        // flatMap
       JavaRDD<String>words= data.flatMap(value->Arrays.asList(value.split(" ")).iterator());
        System.out.println("tu nhi to aisa mai dariya ki jaise ");
        words.collect().forEach(System.out::println);

        // read from disk
        JavaRDD<String>initialRdd=sc.textFile("src/main/java/AUTHORS.txt");
        initialRdd.flatMap(value->Arrays.asList(value.split(" ")).iterator()).collect().forEach(System.out::println);

        JavaRDD<String> filteredWord=words.filter(value-> value.length() > 1);
        filteredWord.collect().forEach(System.out::println);
        sc.close();
    }

}
