package com.jacker.Kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.spark.streaming.Minutes
import org.apache.spark.rdd.PairRDDFunctions


object KafkaStreaming {
  def main(args: Array[String]): Unit = {
     val argsArray = Array("192.168.75.129:2181",
      "grouplogger", "logger", "5")
    val Array(zkQuorum, group, topics, numThreads) = argsArray
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkQuorum)
        // Initialize hBase table if necessary
    val admin = new HBaseAdmin(conf)
    val hbaseTableName = "tbl_message_user"
    if (!admin.isTableAvailable(hbaseTableName)) {
      val tableDesc = new HTableDescriptor(hbaseTableName)
      tableDesc.addFamily(new HColumnDescriptor("user"))
      admin.createTable(tableDesc)
    }
     
    val sparkConf = new SparkConf().setAppName("Kafka2streaming").setMaster("local[3]")
   // val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    //val topicSet = topics.split(",").map((_, numThreads.toInt)).toSet
    val aliveAppEvent = lines.filter { line => !(line.contains("null"))}
    val fields = aliveAppEvent.map { _.split(",") }
    val appEventCount = aliveAppEvent.count
    //val ArrayEvent = Map("view" -> 1, "collection"-> 2, "shopping" -> 3, "buy" -> 4)
   // val mapRDD = sc.parallelize(ArrayEvent.toSeq)
   // val keys = fields.map { x => (x(0),x(1),x(2),x(3)) }.map(x => (x._1 + "|" + x._3)).toString()
    val data = aliveAppEvent.map { line => if(line.contains("view")) {line.replace("view", "1")} 
    else if(line.contains("collection")) {line.replace("collection", "2")}  
    else if(line.contains("shopping")) {line.replace("shopping", "3")} 
    else if(line.contains("buy")) {line.replace("buy", "4")}
    else null}
    //data.print()
    data.map {_.split(",")}.map(x =>((x(0).toString() + "|" + x(1).toString() + "|" +  x(2).toString()),(x(0).toString(),x(1).toString(),x(2).toString(),x(3).toString()))).foreachRDD(rdd => 
    saveToHBase(rdd, zkQuorum, hbaseTableName))
  //  line.foreach(line => saveToHBase(lastdata, zkQuorum, hbaseTableName))
    
    //ArrayEvent.get(key).foreach(println)
    ssc.start()
    ssc.awaitTermination()
    }
 //  def add(a : Int, b : Int) = { (a + b) }

  def saveToHBase(rdd : RDD[(String,(String,String,String,String))], zkQuorum : String, tableName : String) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkQuorum)

    val jobConfig = new JobConf(conf)
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    new PairRDDFunctions(rdd.map { case (key,(value0,value1,value2,value3)) => createHBaseRow(key,value0,value1,value2,value3) }).saveAsHadoopDataset(jobConfig)
  }

  def createHBaseRow(key:String,value0:String,value1:String,value2:String,value3:String) = {
    val record = new Put(Bytes.toBytes(key))
    record.add(Bytes.toBytes("user"), Bytes.toBytes("time"), Bytes.toBytes(value0))
    record.add(Bytes.toBytes("user"), Bytes.toBytes("producer"), Bytes.toBytes(value1))
    record.add(Bytes.toBytes("user"), Bytes.toBytes("useid"), Bytes.toBytes(value2))
    record.add(Bytes.toBytes("user"), Bytes.toBytes("trade"), Bytes.toBytes(value3))
    (new ImmutableBytesWritable, record)
  }

/*  def createNewConnection(){
    val config = HBaseConfiguration.create()
    config.set("hbase.master", "192.168.75.129:6000")
    config.set("hbase.zookeeper.property.clientPort", "2181")
    config.set("hbase.zookeeper.quorum", "192.168.75.129")
    val conn = ConnectionFactory.createConnection(config);
    val admin = conn.getAdmin
    val userTable = TableName.valueOf("tbl_message")
    if(admin.tableExists(userTable)){
      println("table is exists!")
    }else{
      val tableDesc = new HTableDescriptor(userTable)
      tableDesc.addFamily(new HColumnDescriptor("user"))
      admin.createTable(tableDesc)
      println("ok")
    }
    
    
  }*/
}