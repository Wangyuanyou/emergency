package com.emg.spark.test

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by wangyuanyou on 5/24/17.
  */
object TestJDBC {
  def main(args:Array[String]): Unit ={
    val connection_str="jdbc:mysql://localhost:3306/emergency?user=root&password=wangyuanyou"
    classOf[com.mysql.jdbc.Driver]
    val connectdb = DriverManager.getConnection(connection_str)
    val statement = connectdb.createStatement()
    val sql_max="select * from tasks where id = (select max(id) from tasks)"
    val result_max=statement.executeQuery(sql_max)
    result_max.next()
    val taskid=result_max.getInt("id")

    val now = new Date()
    val initialNow  = now.getTime
    val ini_str = initialNow+""
    val timetamp=ini_str.substring(0,10).toLong
    val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now_str:String = sdf.format(new Date((timetamp.toLong*1000l)))

    val sql_detailMax="select * from task_detail where id = (select max(id) from task_detail)"
    val result_detailMax=statement.executeQuery(sql_detailMax)
    result_detailMax.next()

    val detailid=result_detailMax.getInt("id")+1
    val amount=500

    val sql_insert="insert into task_detail values ("+detailid+","+taskid+",\""+now_str+"\","+amount+");"
    println(sql_insert)
    statement.executeUpdate(sql_insert)

    connectdb.close()
  }

}
