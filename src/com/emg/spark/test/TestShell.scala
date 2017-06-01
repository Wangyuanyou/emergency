package com.emg.spark.test
import scala.sys.process._
import java.util.concurrent.{Executors, ExecutorService}
/**
  * Created by wangyuanyou on 5/22/17.
  */
object TestShell {
  def main(args:Array[String]): Unit ={
    val threadPool:ExecutorService=Executors.newFixedThreadPool(2)
    threadPool.execute(new runSpark())
    Thread.sleep(2000)
    threadPool.execute(new runFlume())
  }
  class runSpark()extends Runnable{
    override def run(){
      "spark-submit --class com.emg.spark.test.TestFlume --master spark://127.0.0.1:7077 /home/wangyuanyou/Documents/Documents/Spark/SparkAPP/emergency.jar" !
    }
  }
  class runFlume()extends Runnable{
    override def run(){
      Thread.sleep(2000)
      "flume-ng agent -n agent1 -c conf -f /usr/local/flume/apache-flume-1.6.0-bin/conf/flume-conf.properties" !
    }
  }
}
