package com.spark.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

object SqlDemo_DSL {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName("sparkSql")
    val sc=new SparkContext(conf)
    val sparkSql= new SQLContext(sc)
    val lines =sc.textFile(args(0))
    val personRDD=lines.map(line=>{
      val fds =line.split(",")
      val id =fds(0).toLong
      val name =fds(1)
      val age =fds(2).toInt
      val grade =fds(3).toInt
      Row(id,name,age,grade)
    })
    val schema =StructType(List(
      StructField("id",LongType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("grade",IntegerType,true),
    ))
    val bdf:DataFrame=sparkSql.createDataFrame(personRDD,schema)
    val data:DataFrame =bdf.select("id","name","age","grade")
    data.show()


    sc.stop()
  }
}
