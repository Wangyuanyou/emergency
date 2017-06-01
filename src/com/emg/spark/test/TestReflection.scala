package com.emg.spark.test

/**
  * Created by wangyuanyou on 5/12/17.
  */

class People(id:Int,name:String,age:Int){
  var ID:Int=id
  var Name:String=name
  var Age:Int=age
  def this(){
    this(1,"wang",23)
  }
  def this(id:Int,age:Int){
    this(id,"noName",age)
  }
  def show(): Unit ={
    println(ID+","+Name+","+Age)
  }
}
object TestReflection {
  def main(args:Array[String]):Unit= {
    /*val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule("com.emg.spark.jobs.FileInputJob")
    val obj = runtimeMirror.reflectModule(module)*/
    val clazz = Class.forName("com.emg.spark.test.People")
    //val o = clazz.newInstance()
    val con=clazz.getDeclaredConstructor(classOf[Int],classOf[Int])
    val x : java.lang.Integer = 5
    val y : java.lang.Integer = 56
    val o=con.newInstance(x,y)
    val m1 = clazz.getDeclaredMethod("show")
    m1.invoke(o)

   /* val idF =clazz.getDeclaredField("ID")
    idF.setAccessible(true)
    idF.set(o,3)
    val m1 = clazz.getDeclaredMethod("show")
    m1.invoke(o)
    //m1.invoke(o)*/






  }

}
