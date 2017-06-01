package com.emg.spark.test

import java.io._

import scala.io.Source



/**
  * Created by wangyuanyou on 5/25/17.
  */
object TestFile {
  def main(args:Array[String]):Unit={
    def main(args:Array[String]): Unit ={
      val fileName = "hdfs://localhost:9000/user/test/sparktext.txt"
      val source = Source.fromFile(fileName)
      val lines=source.getLines()
      println(lines.size)
      //for (line <- source.getLines)
        //println(line)
      //source.close()
    }
  /*source.close
  var amount=0
  for(line <- lines){
   println(line)
  }
  println(amount)*/
  }
}
