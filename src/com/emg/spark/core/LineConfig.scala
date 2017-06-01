package com.emg.spark.core

/**
  * Created by wangyuanyou on 5/17/17.
  */
class LineConfig {
  var input: Array[String]=new Array[String](0)
  var output: Array[String]=new Array[String](0)
  var currentJob:String=""
  var currentArgs:String=""
  var nexts:Seq[LineConfig]=Seq()

}
