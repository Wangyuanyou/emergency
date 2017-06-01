package com.emg.spark.test

import com.emg.spark.jobs._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by wangyuanyou on 5/17/17.
  */
object Test {
  def main(args:Array[String]):Unit={
    val conf =new SparkConf()
    conf.setAppName("TestMain")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(sc)
    val hiveContext= new HiveContext(sc)


    val arg1="{\"fileType\":\"txt\",\"filePath\":\"/user/test/sparktext.txt\",\"colName\":\"id name age\",\"colType\":\"int string int\",\"splitMask\":\",\"}"
    val one=new FileInputJob(arg1,sc,hiveContext)
    one.process()


    //test HiveOutput
   // val arg2="{\"database\":\"test\",\"table\":\"students\",\"seqOfCol\":\"id,name,age\"}"
    //val two=new HiveOutputJob(arg2,one.output,sc,hiveContext)
    //two.process()

    val arg3="{\"path\":\"/user/test/forever.txt\"}"
    val three=new HDFSOutputJob(arg3,one.output,sc,hiveContext)
    three.process()
    sc.stop()
  }
}
