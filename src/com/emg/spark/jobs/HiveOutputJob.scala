package com.emg.spark.jobs

import com.emg.spark.core.JobBase
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangyuanyou on 5/17/17.
  */
class HiveOutputJob(val arg:String,val in:ArrayBuffer[DataFrame], val sparkcontext:SparkContext,val sqlcontext :SQLContext )extends JobBase{
  var input: ArrayBuffer[DataFrame]=in
  var output:ArrayBuffer[DataFrame]=new ArrayBuffer[DataFrame]
  val args:String=arg
  var status:Int=0//0表示为开始，1表示进行中，2表示运行结束，-1表示运行错误
  val sc:SparkContext=sparkcontext
  val sqlsc:SQLContext=sqlcontext
  def process(): Unit ={
    val argsMapper = new ObjectMapper()
    val argsParsed = argsMapper.readTree(args)

    val database=argsParsed.path("database").toString().substring(1,argsParsed.path("database").toString().size-1)
    val table=argsParsed.path("table").toString().substring(1,argsParsed.path("table").toString().size-1)
    val seqOfCol=argsParsed.path("seqOfCol").toString().substring(1,argsParsed.path("seqOfCol").toString().size-1)

    input(0).registerTempTable("Input")
    //input(0).show()
    //input(0).printSchema()

    val use_str="use "+database
    val insert_str="insert into "+table+" select "+seqOfCol +" from Input"
   // sqlsc.sql(use_str)
   // sqlsc.sql(insert_str)
  }

  def getOutput():ArrayBuffer[DataFrame]={
    return output
  }



}

