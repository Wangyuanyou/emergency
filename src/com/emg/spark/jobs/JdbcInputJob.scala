package com.emg.spark.jobs

import com.emg.spark.core.JobBase
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangyuanyou on 5/11/17.
  */
class JdbcInputJob(val arg:String,val sparkcontext:SparkContext,val sqlcontext:SQLContext)extends JobBase {
  var input: ArrayBuffer[DataFrame]=new ArrayBuffer[DataFrame]
  var output:ArrayBuffer[DataFrame]=new ArrayBuffer[DataFrame]
  val args:String=arg
  var status:Int=0//0表示为开始，1表示进行中，2表示运行结束，-1表示运行错误
  val sc:SparkContext=sparkcontext
  val sqlsc:SQLContext=sqlcontext

  def this(sparkcontext:SparkContext,sqlcontext:SQLContext){
    this("",sparkcontext,sqlcontext)
  }
  def process():Unit={
    val argsMapper = new ObjectMapper()
    val argsParsed = argsMapper.readTree(args)

    val url=argsParsed.path("url").toString().substring(1,argsParsed.path("url").toString().size-1)
    val dbtable=argsParsed.path("dbtable").toString().substring(1,argsParsed.path("dbtable").toString().size-1)
    val driver=argsParsed.path("driver").toString().substring(1,argsParsed.path("driver").toString().size-1)
    val user=argsParsed.path("user").toString().substring(1,argsParsed.path("user").toString().size-1)
    val password=argsParsed.path("password").toString().substring(1,argsParsed.path("password").toString().size-1)
    val reader=sqlsc.read.format("jdbc")
    reader.option("url",url)
    reader.option("dbtable",dbtable)
    reader.option("driver",driver)
    reader.option("user",user)
    reader.option("password",password)

    val jdbcDF=reader.load()

    output+=jdbcDF
  }
  def getOutput():ArrayBuffer[DataFrame]={
    return output
  }

}
