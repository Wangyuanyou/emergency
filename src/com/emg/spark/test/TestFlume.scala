//时间戳已完成
package com.emg.spark.test

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

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
  * Created by wangyuanyou on 5/18/17.
  */
object TestFlume {
  def main(args:Array[String]):Unit={
    val conf =new SparkConf()
    conf.setAppName("TestFlume")
    conf.setMaster("local[4]")

    val ssc= new StreamingContext(conf, Seconds(1))

    val flumeStream=FlumeUtils.createStream(ssc,"localhost",9999,StorageLevel.MEMORY_ONLY_SER_2)
    val sqlStream=flumeStream.map(event=>new String(event.event.getBody.array()))

    val now = new Date()
    val initialNow  = now.getTime
    val ini_str = initialNow+""
    val timetamp=ini_str.substring(0,10).toLong
    val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val sdf2:SimpleDateFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val now_str:String = sdf.format(new Date((timetamp.toLong*1000l)))
    val filename_str:String = sdf2.format(new Date((timetamp.toLong*1000l)))
    val monitorFile="/home/wangyuanyou/Documents/Documents/Spark/sparkRuning/detailOfIncreae"


    //var i=1
    //val preFinalName="/user/test/goods_history"
    sqlStream.foreachRDD(strRDD=>{
      val timeStrRDD=strRDD.map(x=>now_str+":"+x)
      timeStrRDD.saveAsTextFile("hdfs://localhost:9000/emergency/goods/increseHistory/"+filename_str)

      val amount:Long=strRDD.count()
      if(amount>2) {
        val out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(monitorFile, true)));
        out.write(amount + "\n")
        out.close()
      }
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

    //sqlStream.foreach(x=>println(x))
    //flumeStream.count().map(cnt => "Received " + cnt + " flume events." ).print()

    //flumeStream.print()
    //flumeStream.count.print()
    //flumeStream.count.map((in) => "Received " + in + " flume events.").print

    //Test streaming wordcount
   // val lines= ssc.socketTextStream("localhost", 9999)
    //val words = lines.flatMap(_.split(" "))
    //val pairs = words.map(word => (word, 1))
    //val wordCounts = pairs.reduceByKey(_ + _)
    //wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
    //ssc.stop()

  }

}
