package com.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("javaWordCount");
        SparkContext sc= new SparkContext(conf);
        RDD<String> stringRDD = sc.textFile(args[0], 3);
        JavaRDD<String> rdd = stringRDD.toJavaRDD();
        //切分压平
        JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String lines) throws Exception {
                return Arrays.asList(lines.split(" ")).iterator();
            }
        });
        //分组
        JavaPairRDD<String, Integer> pair = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        //聚合
        JavaPairRDD<String, Integer> byKey = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        //K-V转换位置
        JavaPairRDD<Integer, String> map = byKey.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });
        JavaPairRDD<Integer, String> sort = map.sortByKey(false);
        //位置还原
        JavaPairRDD<String, Integer> result = sort.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });
        result.coalesce(1,true).saveAsTextFile(args[1]);
        sc.stop();
    }
}
