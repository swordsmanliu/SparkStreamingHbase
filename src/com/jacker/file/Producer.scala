package com.jacker.file
import scala.util.Random

import java.util.Date
import java.text.SimpleDateFormat
import java.io.PrintWriter
import java.io.File
import java.io.FileWriter

object Producer {
  
  def main(args: Array[String]): Unit = {
    val num = args(0)
    val writer = new FileWriter("test.txt", true); //当前工程根目录下   
    for(i <- 1 to num.toInt ){
      val now = new Date().getTime
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")  
      val userdate = dateFormat.format(now)//时间
      val cid = Random.nextInt(1000000).toString()//访问者ID
      val product = Random.nextInt(50).toString()//商品
      val appEventArray = Array("view", "collection", "shopping", "buy", "null");
      val appEvent = appEventArray(Random.nextInt(appEventArray.length))//事件
      val line = Array(userdate, cid,product,appEvent).mkString(",")
      Thread.sleep(50)
      writer.write(line+"\n")
      println(line)  
    }
     writer.close() 
  }
} 