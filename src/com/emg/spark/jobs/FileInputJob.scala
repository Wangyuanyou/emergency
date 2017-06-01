package com.emg.spark.jobs

import com.emg.spark.core._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.emg.spark.jobs
import org.apache.spark.sql.hive.HiveContext

import Array._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangyuanyou on 5/8/17.
  */
class FileInputJob(val arg:String,val sparkcontext:SparkContext,val sqlcontext:SQLContext)extends JobBase{
  var input: ArrayBuffer[DataFrame]=new ArrayBuffer[DataFrame]
  var output:ArrayBuffer[DataFrame]=new ArrayBuffer[DataFrame]
  val args:String=arg
  var status:Int=0//0表示为开始，1表示进行中，2表示运行结束，-1表示运行错误
  val sc:SparkContext=sparkcontext
  val sqlsc:SQLContext=sqlcontext




  def readTxt(args:JsonNode):Unit={
    val filePath=args.path("filePath").toString().substring(1,args.path("filePath").toString().size-1)

    val lines = sc.textFile(filePath)

    val splitMask=args.path("splitMask").toString().substring(1,args.path("splitMask").toString().size-1)
    val splited = lines.map(x => x.split(splitMask))

    //val rowRDD=splited.map(split=>Row(split(0).trim().toInt,split(1),split(2).trim().toInt))
    val rowRDD=splited.map(split=> Row.fromSeq(split.toSeq))

    val colName=args.path("colName").toString().substring(1,args.path("colName").toString().size-1)
    val fields= colName.split(" ")
      .map(colName => StructField(colName, StringType, nullable = true))

    val schema=StructType(fields)
    //val schema= StructType(Seq(StructField("id",StringType,true),StructField("name",StringType,true),StructField("age",StringType,true)))

    //构造输入的INput
    val initialDF=sqlsc.createDataFrame(rowRDD,schema)
    var tranInput=new ArrayBuffer[DataFrame]
    tranInput+=initialDF

    //构造输入的参数
    var tranArgs="{\"colName\":\""
    var typeIndexBuffer=new ArrayBuffer[Int]
    val colType=args.path("colType").toString().substring(1,args.path("colType").toString().size-1)
    val typeArray=colType.split(" ")
    val nameArray=colName.split(" ")
    var i=0;
    while(i<typeArray.size){
      if(typeArray(i)!="string")
        typeIndexBuffer+=i
      i=i+1
    }
    i=0
    while(i<typeIndexBuffer.size){
      if(i==0)
        tranArgs=tranArgs+nameArray(typeIndexBuffer(i))
      else
        tranArgs=tranArgs+" "+nameArray(typeIndexBuffer(i))
      i=i+1
    }

    tranArgs=tranArgs+"\",\"colType\":\""
    i=0
    while(i<typeIndexBuffer.size){
      if(i==0)
        tranArgs=tranArgs+typeArray(typeIndexBuffer(i))
      else
        tranArgs=tranArgs+" "+typeArray(typeIndexBuffer(i))
      i=i+1
    }
    tranArgs=tranArgs+"\"}"

    val doTran=new ColDataTypeTranJob(tranArgs,tranInput)
    doTran.process()

    output+=doTran.output(0)
  }

  def readJson(args:JsonNode):Unit={
    val filePath=args.path("filePath").toString().substring(1,args.path("filePath").toString().size-1)
    val readDF=sqlsc.read.format("json").load(filePath)
    output+=readDF
  }

    def process():Unit= {
    val argsMapper = new ObjectMapper()
    val argsParsed = argsMapper.readTree(args)
    val fileType=argsParsed.path("fileType").toString().substring(1,argsParsed.path("fileType").toString().size-1)
    val toDo=fileType match {
      case "txt" => readTxt(argsParsed)
      case "json"=> readJson(argsParsed)
    }
  }
  def getOutput():ArrayBuffer[DataFrame]={
    return output
  }
}
