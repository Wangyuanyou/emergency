package com.emg.spark.test

/**
  * Created by wangyuanyou on 5/7/17.
  */
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._




object DataFrameAPP {
  def main (args: Array[String]):Unit={
    val conf =new SparkConf()
    conf.setAppName("My first try on DataFrame")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val lines =sc.textFile("/home/wangyuanyou/Documents/Documents/Spark/sparkData/sparktext.txt")
    val splited = lines.map(x => x.split(","))
    val rowRDD=splited.map(split=> Row.fromSeq(split.toSeq))

    val colName="id name age"
    val colType="int string int"

    val fields= colName.split(" ")
      .map(colName => StructField(colName, StringType, nullable = true))
    val schema=StructType(fields)
    val df=sqlContext.createDataFrame(rowRDD,schema)

    val newdf=df.select(df.col("id").cast(IntegerType)as("id"),df.col("name"),df.col("age").cast(IntegerType)as("age"))

    newdf.show()
    newdf.printSchema()


    sc.stop()
  }


}
