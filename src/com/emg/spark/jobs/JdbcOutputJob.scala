package com.emg.spark.jobs

import java.sql.DriverManager

import com.emg.spark.core.JobBase
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import java.util.Properties

import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangyuanyou on 5/12/17.
  */
class JdbcOutputJob(val arg:String,val in:ArrayBuffer[DataFrame], val sparkcontext:SparkContext,val sqlcontext:SQLContext)extends JobBase {
  var input: ArrayBuffer[DataFrame]=in
  var output:ArrayBuffer[DataFrame]=new ArrayBuffer[DataFrame]
  val args:String=arg
  var status:Int=0//0表示为开始，1表示进行中，2表示运行结束，-1表示运行错误
  val sc:SparkContext=sparkcontext
  val sqlsc:SQLContext=sqlcontext

  def process():Unit={
    val argsMapper = new ObjectMapper()
    val argsParsed = argsMapper.readTree(args)

    val url=argsParsed.path("url").toString().substring(1,argsParsed.path("url").toString().size-1)
    val dbtable=argsParsed.path("dbtable").toString().substring(1,argsParsed.path("dbtable").toString().size-1)
    val user=argsParsed.path("user").toString().substring(1,argsParsed.path("user").toString().size-1)
    val password=argsParsed.path("password").toString().substring(1,argsParsed.path("password").toString().size-1)
    val savetype=argsParsed.path("savetype").toString().substring(1,argsParsed.path("savetype").toString().size-1)
    val seqOfCol=argsParsed.path("seqOfCol").toString().substring(1,argsParsed.path("seqOfCol").toString().size-1)

    val oldDF=input(0)
    oldDF.registerTempTable("wrongOrder")
    val sql_str="select "+seqOfCol +" from wrongOrder"
    val goodDF=sqlsc.sql(sql_str)

    val prop=new Properties()
    prop.setProperty("user",user)
    prop.setProperty("password",password)

    val writter= savetype match {
      case overwrite=>goodDF.write.mode(SaveMode.Overwrite)
      case append=>goodDF.write.mode(SaveMode.Append)
    }
    writter.jdbc(url,dbtable,prop)



  }
  def getOutput():ArrayBuffer[DataFrame]={
    return output

  }

}
