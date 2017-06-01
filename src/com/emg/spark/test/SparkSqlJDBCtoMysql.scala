package com.emg.spark.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangyuanyou on 5/7/17.
  */
object SparkSqlJDBCtoMysql {
  def main(args:Array[String]):Unit={
    val conf =new SparkConf()
    conf.setAppName("SparkSQL to Mysql")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val reader =sqlContext.read.format("jdbc")
    reader.option("url","jdbc:mysql://localhost:3306/test")
    reader.option("dbtable","students")
    reader.option("driver","com.mysql.jdbc.Driver")
    reader.option("user","root")
    reader.option("password","wangyuanyou")

    val mysqlDF=reader.load()
    mysqlDF.select("age").show()
    sc.stop()
  }

}
