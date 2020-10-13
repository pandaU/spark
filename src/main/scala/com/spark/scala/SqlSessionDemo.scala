package com.spark.scala


import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}


object SqlSessionDemo {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("sparkSession_2.x").getOrCreate()
    val lines=spark.sparkContext.textFile(args(0))
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
    val df: DataFrame = spark.createDataFrame(personRDD, schema)
    import spark.implicits._
    val result: Dataset[Row] = df.where($"id" > 6).orderBy($"id" desc)
    //result.write.jdbc()
    result.show()
    spark.stop()
  }
}
