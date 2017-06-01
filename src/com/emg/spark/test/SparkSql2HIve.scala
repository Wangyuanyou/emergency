package com.emg.spark.test

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by wangyuanyou on 5/8/17.
  */
object SparkSql2HIve {
  def main(args:Array[String]):Unit={
    val conf =new SparkConf()
    conf.setAppName("SparkSQL to Hive")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    val lines = sc.textFile("/home/wangyuanyou/Documents/Documents/Spark/sparkData/sparktext.txt")
    val splited = lines.map(x => x.split(","))
    val  rowRDD =splited.map(splited=>Row(splited(0).trim().toInt,splited(1),splited(2).trim().toInt))
    val schema = StructType(Seq(StructField("id",IntegerType,true),StructField("name",StringType,true),StructField("age",IntegerType,true)))
    val df=hiveContext.createDataFrame(rowRDD,schema)
    df.show()
    df.printSchema()
   // df.registerTempTable("studentsInput")

    //hiveContext.sql("use test")
    //hiveContext.sql("insert into students select * from studentsInput")
    //hiveContext.sql("select * from students").show()



    //val stuDF=hiveContext.table("students")
    //stuDF.show()


    //hiveContext.sql("show tables;").show()

    sc.stop()

  }

}
