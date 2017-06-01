package com.emg.spark.core

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.sql.DriverManager

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import java.sql.Statement

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangyuanyou on 5/17/17.
  */
class ETLLine(val sour:String,val confpath:String,val sparkContext:SparkContext,val hiveContext:SQLContext ){
  val source:String=sour
  var target:Array[String]=new Array[String](0)
  val configPath:String=confpath
  var parsedConfig:LineConfig=new LineConfig()
  var status:Int=0//0表示为开始，1表示进行中，2表示运行结束，-1表示运行错误
  val sc:SparkContext=sparkContext
  val hivesc:SQLContext=hiveContext

  def parseConfig():Unit={
    val parser= new ConfigParser(configPath)
    parser.lineParser
    parsedConfig=parser.parsedConfig
    target=parser.target
  }
  def doProcess(currentConf: LineConfig,statement: Statement,tmpPut:ArrayBuffer[DataFrame]):Unit={
    val state_str_pre="select * from jobLib where jobname=\""
    val jobname=currentConf.currentJob
    var state_str=state_str_pre+jobname+"\""
    val result=statement.executeQuery(state_str)
    result.next()

   // println(result.getString("jobpath"))
    val jobCl = Class.forName(result.getString("jobpath"))

    val jobType=result.getString("jobtype")
    val jobConstructor= jobType match{
      case "input" =>jobCl.getDeclaredConstructor(classOf[String],classOf[SparkContext],classOf[SQLContext])
      case "tran"  =>jobCl.getDeclaredConstructor(classOf[String],classOf[ArrayBuffer[DataFrame]])
      case "output"=>jobCl.getDeclaredConstructor(classOf[String],classOf[ArrayBuffer[DataFrame]],classOf[SparkContext],classOf[SQLContext])
    }
    val jobOJ= jobType match{
      case "input"=>jobConstructor.newInstance(currentConf.currentArgs,sc,hivesc)
      case "tran"  =>jobConstructor.newInstance(currentConf.currentArgs,tmpPut)
      case "output"=>jobConstructor.newInstance(currentConf.currentArgs,tmpPut,sc,hivesc)
    }
    val processMD = jobCl.getDeclaredMethod("process")
    processMD.invoke(jobOJ)
    val getOutputMD=jobCl.getDeclaredMethod("getOutput")
    val newTmpPut:ArrayBuffer[DataFrame]=classOf[ArrayBuffer[DataFrame]].cast(getOutputMD.invoke(jobOJ))
    //val newTmpPut:ArrayBuffer[DataFrame]=new ArrayBuffer[DataFrame]
    //如果是input 记录读入的数据量
    if(jobType=="input"){
      val amount=newTmpPut(0).count()
      val amountFile="/home/wangyuanyou/Documents/Documents/Spark/sparkRuning/amountFull"
      val out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(amountFile, true)));
      out.write(amount + "\n")
      out.close()
    }
    for(next<-currentConf.nexts)
      doProcess(next,statement,newTmpPut)
  }
  def run:Unit={
    status=1
    val connection_str="jdbc:mysql://localhost:3306/emergency?user=root&password=wangyuanyou"
    classOf[com.mysql.jdbc.Driver]
    val connectdb = DriverManager.getConnection(connection_str)
    val statement = connectdb.createStatement()

    val tmpPut=new ArrayBuffer[DataFrame]

    doProcess(parsedConfig,statement,tmpPut)
    connectdb.close()
  }


}
