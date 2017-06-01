package com.emg.spark.jobs

import com.emg.spark.core.JobBase
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangyuanyou on 5/9/17.
  */
class ColDataTypeTranJob(val arg:String,val in:ArrayBuffer[DataFrame]) extends JobBase{
  var input:ArrayBuffer[DataFrame]=in
  var output:ArrayBuffer[DataFrame]=new ArrayBuffer[DataFrame]
  val args:String=arg
  var status:Int=0//0表示为开始，1表示进行中，2表示运行结束，-1表示运行错误

  def process():Unit={
    val argsMapper = new ObjectMapper()
    val argsParsed = argsMapper.readTree(args)
    val colName=argsParsed.path("colName").toString().substring(1,argsParsed.path("colName").toString().size-1)
    val colType=argsParsed.path("colType").toString().substring(1,argsParsed.path("colType").toString().size-1)

    val nameArray=colName.split(" ")
    val typeArray=colType.split(" ")

    var i=0
    var tmpDF=input(0)

    while(i<nameArray.size){
      val field=nameArray(i)
      val tmpNoDrop= typeArray(i) match{
        case "int"=>tmpDF.withColumn("tmp",tmpDF.col(field).cast(IntegerType))
        case "double"=>tmpDF.withColumn("tmp",tmpDF.col(field).cast(DoubleType))
      }
      val tmpDrop=tmpNoDrop.drop(field)
      tmpDF=tmpDrop.withColumnRenamed("tmp",field)
      i=i+1
    }
    output+=tmpDF
  }
  def getOutput():ArrayBuffer[DataFrame]={
    return output
  }

}
