package com.spark.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

object SqlDemo_DSL {
  def main(args: Array[String]): Unit = {
    var conf =new SparkConf().setAppName("sparkSql")
    var sc=new SparkContext(conf)
    var sparkSql= new SQLContext(sc)
    var lines =sc.textFile(args(0))
    var personRDD=lines.map(line=>{
      var fds =line.split(",")
      var id =fds(0).toLong
      var name =fds(1)
      var age =fds(2).toInt
      var grade =fds(3).toInt
      Row(id,name,age,grade)
    })
    var schema =StructType(List(
      StructField("id",LongType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("grade",IntegerType,true),
    ))
    var bdf:DataFrame=sparkSql.createDataFrame(personRDD,schema)

  }
}