package com.emg.spark.core

import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangyuanyou on 5/8/17.
  */
trait JobBase {
  var input:ArrayBuffer[DataFrame]
  var output:ArrayBuffer[DataFrame]
  val args:String
  var status:Int

  def process():Unit
}
