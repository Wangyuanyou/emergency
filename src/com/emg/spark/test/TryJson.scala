package com.emg.spark.test


import java.io.File
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
import com.emg.spark.core
import com.emg.spark.core.{ConfigParser, ETLLine, LineConfig}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source


/**
  * Created by wangyuanyou on 5/8/17.
  */
object TryJson {
  def printConfig(config: LineConfig):Unit={
    println(config.input.toString())
    println(config.output.toString())
    println(config.currentJob)
    println(config.currentArgs)
    println(config.nexts.size)
    for(next<-config.nexts)
      printConfig(next)
  }

  def main(args:Array[String]):Unit={
    //val args:String="{\"fileType\":\"txt\",\"filePath\":\"/home/wangyuanyou/Documents/Documents/Spark/sparkData/sparktext.txt\",\"colName\":\"id name age\",\"colType\":\"int string int\",\"splitMask\":\",\"}"

    //val json="{\"name\":\"Kris\",\"age\":27,\"score\":85}"
    //case class Student(name:String,age:Int,score:Int)

    //val mapper = new ObjectMapper()
    //val oneStudent=mapper.readValue(json,classOf[Student])
    //println(oneStudent.toString())
    val file="/home/wangyuanyou/Documents/Documents/Spark/sparkData/config/LineConfigSample.json"
    //val oneParser=new ConfigParser(file)
    //oneParser.lineParser
    //val config=oneParser.parsedConfig
    //printConfig(config)
    //val targets=oneParser.target
    //for(target<-targets)
    //println(t

    val conf =new SparkConf()
    conf.setAppName("TestMain")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(sc)
    val hiveContext= new SQLContext(sc)

    val oneETL=new ETLLine("st",file,sc,hiveContext)
    oneETL.parseConfig()
    oneETL.run

    sc.stop()
    //get taskid
    val connection_str="jdbc:mysql://localhost:3306/emergency?user=root&password=wangyuanyou"
    classOf[com.mysql.jdbc.Driver]
    val connectdb = DriverManager.getConnection(connection_str)
    val statement = connectdb.createStatement()
    val sql="select * from tasks where state = 1"
    val result=statement.executeQuery(sql)

    if(result.next()==false)
      return
    val taskid=result.getInt("id")
    // get amount
    val fileName = "/home/wangyuanyou/Documents/Documents/Spark/sparkRuning/amountFull"
    val source = Source.fromFile(fileName)
    var amount=0
    for (line <- source.getLines)
      amount=amount+line.toInt
    source.close
    //get end_time
    val now = new Date()
    val initialNow  = now.getTime
    val ini_str = initialNow+""
    val timetamp=ini_str.substring(0,10).toLong
    val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss")
    val now_str:String = sdf.format(new Date((timetamp.toLong*1000l)))


    val update_str="update tasks set state=0,time_end=\""+now_str+"\",amount="+amount+" where id ="+taskid
    statement.executeUpdate(update_str)
    connectdb.close()
    /*val mapper = new ObjectMapper()
    val root= mapper.readTree(new File(file))
    val next=root.path("next")
    val next1=next.path("next1")
    val next2=next.path("next2")
    println(root.path("job").toString)
    println(next1.path("job").toString)
    println(next2.path("job").toString)*/
  }


}
