package com.emg.spark.core

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.DeserializationFeature
import java.io.File

/**
  * Created by wangyuanyou on 5/17/17.
  */
class ConfigParser(configfile:String) {
  val configFile:String=configfile
  var parsedConfig:LineConfig= new LineConfig()
  var target:Array[String]=new Array[String](0)


  def Parser(currentNode:JsonNode):LineConfig= {
    val currentConfig = new LineConfig()
    currentConfig.currentJob = currentNode.path("job").toString().substring(1, currentNode.path("job").toString().size - 1)
    currentConfig.currentArgs = currentNode.path("args").toString

    val input_quote = currentNode.path("input").toString()
    if (input_quote != "\"\"") {
      val input_str = currentNode.path("input").toString().substring(1, currentNode.path("input").toString().size - 1)
      currentConfig.input = input_str.split(" ")
    }
    val output_quote = currentNode.path("output").toString()
    if (input_quote != "\"\"") {
      val output_str = currentNode.path("output").toString().substring(1, currentNode.path("output").toString().size - 1)
      currentConfig.output = output_str.split(" ")
    }


    val currentNext=currentNode.path("next")
    val nextLineNum=currentNext.path("num").asInt()
    var tmpNexts:Seq[LineConfig]=Seq()
    if (nextLineNum==0){
      val mapper = new ObjectMapper()
      val root= mapper.readTree(currentConfig.currentArgs)

      val oneTarget=  currentConfig.currentJob match{
        case "JdbcOutputJob"=>"database:"+root.path("url").toString
        case "HiveOutputJob"=>"hive:"+root.path("table").toString
        case "HDFSOutputJob"=>"HDFS:"+root.path("path").toString
      }
      target=target:+oneTarget
    }

    var i=1
    while(i<=nextLineNum){
      val next_str="next"+i
      val nextNode=Parser(currentNext.path(next_str))
      tmpNexts=tmpNexts:+ nextNode
      i=i+1
      }
    currentConfig.nexts=tmpNexts
    return currentConfig
    }

  def lineParser:Unit={
    val mapper = new ObjectMapper()
    val root= mapper.readTree(new File(configFile))
    parsedConfig=Parser(root)
  }

}
