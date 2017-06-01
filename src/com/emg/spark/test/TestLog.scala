package com.emg.spark.test

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
  * Created by wangyuanyou on 5/24/17.
  */
object TestLog {
  def main(args:Array[String]):Unit={
    val conf =new SparkConf()
    conf.setAppName("TestLogistic")
    conf.setMaster("local[4]")

    val ssc= new StreamingContext(conf, Seconds(5))

    val flumeStream=FlumeUtils.createStream(ssc,"localhost",9998,StorageLevel.MEMORY_ONLY_SER_2)
    val sqlStream=flumeStream.map(event=>new String(event.event.getBody.array()))


    val Maxconnection_str="jdbc:mysql://localhost:3306/emergency?user=root&password=wangyuanyou"
    classOf[com.mysql.jdbc.Driver]
    val Maxconnectdb = DriverManager.getConnection(Maxconnection_str)
    val Maxstatement = Maxconnectdb.createStatement()
    val sql_max="select * from tasks where id = (select max(id) from tasks)"
    val result_max=Maxstatement.executeQuery(sql_max)
    result_max.next()
    val taskid=result_max.getInt("id")
    Maxconnectdb.close()
    //var i=1
    //val preFinalName="/user/test/goods_history"
    var amount:Long=0
    sqlStream.foreachRDD(strRDD=>{
      strRDD.saveAsTextFile("hdfs://localhost:9000/user/test/logisticHisotry")

      amount=strRDD.count()

      if(amount > 2){
        val connection_str = "jdbc:mysql://localhost:3306/emergency?user=root&password=wangyuanyou"
        classOf[com.mysql.jdbc.Driver]
        val connectdb = DriverManager.getConnection(connection_str)
        val statement = connectdb.createStatement()

        val now = new Date()
        val initialNow = now.getTime
        val ini_str = initialNow + ""
        val timetamp = ini_str.substring(0, 10).toLong
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val now_str: String = sdf.format(new Date((timetamp.toLong * 1000l)))

        val sql_detailMax = "select * from task_detail where id = (select max(id) from task_detail)"
        val result_detailMax = statement.executeQuery(sql_detailMax)
        result_detailMax.next()

        val detailid = result_detailMax.getInt("id") + 1
        val sql_insert = "insert into task_detail values (" + detailid + "," + taskid + ",\"" + now_str + "\"," + amount + ");"
        println(sql_insert)
        statement.executeUpdate(sql_insert)
        connectdb.close()
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
    ssc.start()
    ssc.awaitTermination()
  }

}
