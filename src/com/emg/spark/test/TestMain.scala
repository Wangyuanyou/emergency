package com.emg.spark.test
import java.sql.DriverManager

import com.emg.spark.jobs._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangyuanyou on 5/8/17.
  */
object TestMain {
  def main(args:Array[String]):Unit={
    val conf =new SparkConf()
    conf.setAppName("TestMain")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val connection_str="jdbc:mysql://localhost:3306/emergency?user=root&password=wangyuanyou"
    classOf[com.mysql.jdbc.Driver]
    val connectdb = DriverManager.getConnection(connection_str)
    val statement = connectdb.createStatement()
    val state_str_pre="select * from jobLib where jobname=\""


    val jobname="JdbcInputJob"
    var state_str=state_str_pre+jobname+"\""
    val result=statement.executeQuery(state_str)
    result.next()

    val arg1="{\"url\":\"jdbc:mysql://localhost:3306/test\",\"dbtable\":\"students\",\"driver\":\"com.mysql.jdbc.Driver\",\"user\":\"root\",\"password\":\"wangyuanyou\"}"
    val jobCl = Class.forName(result.getString("jobpath"))
    val jobConstructor=jobCl.getDeclaredConstructor(classOf[String],classOf[SparkContext],classOf[SQLContext])
    val jobOJ=jobConstructor.newInstance(arg1,sc,sqlContext)
    val processMD = jobCl.getDeclaredMethod("process")
    processMD.invoke(jobOJ)
    val getOutputMD=jobCl.getDeclaredMethod("getOutput")
    val out:ArrayBuffer[DataFrame]= classOf[ArrayBuffer[DataFrame]].cast(getOutputMD.invoke(jobOJ))
    out(0).show()





    //val arg2="{\"savetype\":\"overwrite\",\"url\":\"jdbc:mysql://localhost:3306/test\",\"dbtable\":\"newstudents\",\"user\":\"root\",\"password\":\"wangyuanyou\"}"
   // {"savetype":"overwrite","url":"jdbc:mysql://localhost:3306/test","dbtable":"newstudents","user":"root","password":"wangyuanyou"}


    /*val one= new JdbcInputJob(arg1,sc,sqlContext)
    one.process()

    val arg2="{\"savetype\":\"overwrite\",\"url\":\"jdbc:mysql://localhost:3306/test\",\"dbtable\":\"newstudents\",\"user\":\"root\",\"password\":\"wangyuanyou\"}"

    val two = new JdbcOutputJob(arg2,one.output,sc,sqlContext)
    two.process()*/
    sc.stop()
  }

}
