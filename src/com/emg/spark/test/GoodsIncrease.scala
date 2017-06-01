package com.emg.spark.test

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.storage.StorageLevel
import spark.jobserver.{SparkJob, SparkJobValid, SparkJobValidation}
/**
  * Created by wangyuanyou on 5/24/17.
  */
object GoodsIncrease  extends SparkJob {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[4]", "goodsIncrease")
    val config = ConfigFactory.parseString("")
    runJob(sc, config)
  }
  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid
  }
  override def runJob(sc: SparkContext, config: Config): Any = {
    val ssc= new StreamingContext(sc, Seconds(1))

    val flumeStream=FlumeUtils.createStream(ssc,"localhost",9999,StorageLevel.MEMORY_ONLY_SER_2)
    val sqlStream=flumeStream.map(event=>new String(event.event.getBody.array()))
    //var i=1
    //val preFinalName="/user/test/goods_history"
    sqlStream.foreachRDD(strRDD=>{
      strRDD.saveAsTextFile("hdfs://localhost:9000/user/test/goods_history")
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
