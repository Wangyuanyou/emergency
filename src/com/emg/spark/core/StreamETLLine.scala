package com.emg.spark.core

import java.sql.DriverManager

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.flume.FlumeUtils

/**
  * Created by wangyuanyou on 5/18/17.
  */
class StreamETLLine(val sour:String,val confpath:String,val ssc:StreamingContext) {
  val source:String=sour
  var target:Array[String]=new Array[String](0)
  val configPath:String=confpath
  var parsedConfig:LineConfig=new LineConfig()
  var status:Int=0//0表示为开始，1表示进行中，2表示运行结束，-1表示运行错误
  val streamsc:StreamingContext=ssc
  def parseConfig():Unit={
    val parser= new ConfigParser(configPath)
    parser.lineParser
    parsedConfig=parser.parsedConfig
    target=parser.target
  }
  def run:Unit={
    val flumeStream=FlumeUtils.createStream(ssc,"localhost",9999,StorageLevel.MEMORY_ONLY_SER_2)
    val sqlStream=flumeStream.map(event=>new String(event.event.getBody.array()))

    sqlStream.foreachRDD(strRDD=>{
      val sqlContext = SQLContext.getOrCreate(strRDD.sparkContext)
      import sqlContext.implicits._
      val sqlDF = strRDD.toDF("sql")
      val connection_str="jdbc:mysql://localhost:3306/emergency?user=root&password=wangyuanyou"
      classOf[com.mysql.jdbc.Driver]
      val connectdb = DriverManager.getConnection(connection_str)
      val statement = connectdb.createStatement()
      strRDD.foreach(x=>statement.executeQuery(x))
    })
    streamsc.start()
    streamsc.awaitTermination()
  }
}
