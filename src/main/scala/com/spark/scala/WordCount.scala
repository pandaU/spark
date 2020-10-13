package com.spark.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    print("helloWord")
    var conf =new SparkConf().setAppName("scalaWordCount")
    var sc =new SparkContext(conf)
    var lines: RDD[String] = sc.textFile(args(0))
    var word: RDD[String] =lines.flatMap(_.split(" "))
    var map: RDD[(String,Int)] =word.map((_,1))
    var result: RDD[(String,Int)] =map.reduceByKey(_+_)
    var sort: RDD[(String,Int)] =result.sortBy(_._2)
    sort.saveAsTextFile(args(1))
    sc.stop()
  }
}
