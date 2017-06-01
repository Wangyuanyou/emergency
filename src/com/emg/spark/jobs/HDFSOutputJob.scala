package com.emg.spark.jobs

import java.text.SimpleDateFormat
import java.util.Date

import com.emg.spark.core.JobBase
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangyuanyou on 5/17/17.
  */
class HDFSOutputJob (val arg:String,val in:ArrayBuffer[DataFrame], val sparkcontext:SparkContext,val sqlcontext :SQLContext )extends JobBase{
  var input: ArrayBuffer[DataFrame]=in
  var output:ArrayBuffer[DataFrame]=new ArrayBuffer[DataFrame]
  val args:String=arg
  var status:Int=0//0表示为开始，1表示进行中，2表示运行结束，-1表示运行错误
  val sc:SparkContext=sparkcontext
  val sqlsc:SQLContext=sqlcontext


  def process():Unit={
    val argsMapper = new ObjectMapper()
    val argsParsed = argsMapper.readTree(args)
    val path=argsParsed.path("path").toString().substring(1,argsParsed.path("path").toString().size-1)
    val rdd=input(0).javaRDD
    val strArray:Array[String]=rdd.collect().toArray.map(x=>x.toString)
    val strRdd:RDD[String]= sc.parallelize(strArray)

    val now = new Date()
    val initialNow  = now.getTime
    val ini_str = initialNow+""
    val timetamp=ini_str.substring(0,10).toLong
    val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss")
    val sdf2:SimpleDateFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val now_str:String = sdf.format(new Date((timetamp.toLong*1000l)))
    val filename_str:String = sdf2.format(new Date((timetamp.toLong*1000l)))

    val timeRDD:RDD[String]=strRdd.map(x=>now_str+":"+x)
    val timePath=path+"/"+filename_str

    timeRDD.saveAsTextFile(timePath)
  }
  def getOutput():ArrayBuffer[DataFrame]={
    return output
  }


}
