package com.spark.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    print("helloWord")
    val conf =new SparkConf().setAppName("scalaWordCount")
    val sc =new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))
    val word: RDD[String] =lines.flatMap(_.split(" "))
    val map: RDD[(String,Int)] =word.map((_,1))
    val result: RDD[(String,Int)] =map.reduceByKey(_+_)
    val sort: RDD[(String,Int)] =result.sortBy(_._2)
    sort.saveAsTextFile(args(1))
    sc.stop()
  }
}
