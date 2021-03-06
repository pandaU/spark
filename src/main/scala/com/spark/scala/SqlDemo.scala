package com.spark.scala

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SqlDemo {
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
    bdf.registerTempTable("t_person")
    val data : DataFrame=sparkSql.sql("select * from t_person order by grade desc ,age asc")
    data.show()
    sc.stop()
  }
}
