package com.emg.spark.test
import org.apache.spark.sql._
/**
  * Created by wangyuanyou on 5/9/17.
  */
object TestRow {
  /*def string2type(x:String,colType:String):Object={
    val traned:Object= colType match{
      case "int" =>x.trim().toInt
      case "String"=>x
      case "float"=>x.trim().toFloat
      case " bool"=>x.trim().toBoolean
      case "double"=>x.trim().toDouble
    }
    return traned

  }*/

  def main(args:Array[String]):Unit={
    val colType="int string int"
    val origin = new Array[String](3)
    origin(0)="1"
    origin(1)="spark"
    origin(2)="15"
    val splited=colType.split(" ")
    var traned=Seq()

    var i=0
    while(i<splited.size){
      //println(splited(i))
        val toDo  = splited(i) match{
        case "int"=>traned :+ origin(i).trim().toInt
        case "string"=>traned :+origin(i)
      }
      i=i+1
    }
    println(traned.size)

  }
}
