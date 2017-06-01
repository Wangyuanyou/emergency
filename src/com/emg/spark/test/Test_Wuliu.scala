package com.emg.spark.test

import java.sql.DriverManager

import org.apache.hadoop.hive.ql.exec.spark.session._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.storage.StorageLevel
/**
  * Created by wangyuanyou on 5/24/17.
  */
object TestLogistic {
  def main(args:Array[String]):Unit={
    val conf =new SparkConf()
    conf.setAppName("TestLogistic")
    conf.setMaster("local[4]")

    val ssc= new StreamingContext(conf, Seconds(1))

    val flumeStream=FlumeUtils.createStream(ssc,"localhost",9998,StorageLevel.MEMORY_ONLY_SER_2)
    val sqlStream=flumeStream.map(event=>new String(event.event.getBody.array()))
    //var i=1
    //val preFinalName="/user/test/goods_history"
    sqlStream.foreachRDD(strRDD=>{
      strRDD.saveAsTextFile("hdfs://localhost:9000/user/test/logisticHisotry")
      //转换为Dataframe
      //val sqlContext = SQLContext.getOrCreate(strRDD.sparkContext)
      //import sqlContext.implicits._
      //val sqlDF = strRDD.toDF("sql_state")

      //数据库操作
      //val connection_str="jdbc:mysql://localhost:3306/emergency?user=root&password=wangyuanyou"
      //classOf[com.mysql.jdbc.Driver]
      //val connectdb = DriverManager.getConnection(connection_str)
      //val statement = connectdb.createStatement()
      //strRDD.foreach(x=>statement.executeQuery(x))
    })

    ssc.start()
    ssc.awaitTermination()
  }


}
