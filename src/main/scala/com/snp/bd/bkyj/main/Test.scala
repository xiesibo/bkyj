package com.snp.bd.bkyj.main

import java.io.{IOException, File, StringWriter, FileInputStream}
import java.net.{URI, URLDecoder}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.regex.Pattern
import java.util.{UUID, Properties, Calendar, Date}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Scan, HTable, Put, ConnectionFactory}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import scala.actors.migration.pattern
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.matching.Regex


/**
  * Created by Administrator on 2017/11/27.
  */
object Test {
  val sparkConf = new SparkConf().setAppName("testhbase").setMaster("yarn-client").set("spark.inputFormat.cache.enabled", "false").set("spark.hbase.obtainToken.enabled", "true").set("spark.sql.authorization.enabled", "true").set("hive.security.authorization.enabled", "true")
  val sc = new SparkContext(sparkConf)
  def main(args: Array[String]): Unit =
{
  val sqlContext=new SQLContext(sc)
  val hconf = HBaseConfiguration.create();
  val connection: Connection = ConnectionFactory.createConnection(hconf)

  // Declare the description of the table
      val userTable = TableName.valueOf("xsb12")
      val tableDescr = new HTableDescriptor(userTable)
      tableDescr.addFamily(new HColumnDescriptor("info".getBytes))

  println("Creating table shb1. ")
      val admin = connection.getAdmin
      if (admin.tableExists(userTable)) {
        admin.disableTable(userTable)
        admin.deleteTable(userTable)
      }
      admin.createTable(tableDescr)

      connection.close()
      println("Done!")
  println("*****129")
  val userData = Array("2017-01-01,002,,1999,2017-01-01 06:02:20",
    "2017-01-03,001,http://www.souhu.com/,1699,2017-01-03 06:02:20",
    "2017-01-01,003,http://www.spark.com/,1099,2017-01-05 06:02:20",
    "2017-01-01,004,http://www.kafka.com/,1799,2017-01-06 06:02:20",
    "2017-02-02,005,http://www.kafka.com/,1099,2017-01-08 06:02:20",
    "2017-01-02,006, ,1799,2017-01-01 06:02:20"
  )
  val userRDD = sc.parallelize(userData) //生成rdd
val userRddROW = userRDD.map(row => {
    val split = row.split(",");
    org.apache.spark.sql.Row(split(0), split(1).toInt, split(2), split(3).toInt, split(4))
  })
  val userStruct = StructType(Array(
    StructField("time", StringType, true),
    StructField("id", IntegerType, true),
    StructField("url", StringType, true),
    StructField("count", IntegerType, true),
    StructField("time1", StringType, true)
  ))
  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val data = sqlContext.createDataFrame(userRddROW, userStruct) //生成dataFrame
  val ColumnsNames="time,id".split(",")
  data.rdd.foreachPartition(x => {
    val columnFamily="info";
    val putList = new java.util.ArrayList[Put]()
    for(row<-x){
      val put = new Put(Bytes.toBytes(UUID.randomUUID().toString))
      for (i <- 0 to ColumnsNames.size - 1) {
        if (row.get(i) != null) {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(ColumnsNames(i)), Bytes.toBytes(row.get(i).toString))
        } else {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(ColumnsNames(i)), null)
        }
      }
      putList.add(put)
    }
    var table: Table = null
    var connection: Connection = null
    val conf = HBaseConfiguration.create()
    connection = ConnectionFactory.createConnection(conf)
    table = connection.getTable(TableName.valueOf("xsb12"))
    if (putList.size() > 0) {
      table.put(putList)
    }
    if (connection != null) {
      try {
        //关闭Hbase连接.
        connection.close()
      } catch {
        case e: IOException =>
          e.printStackTrace()
      }
    }
  })
  println("*****163")
  //getTableNm("xsb1",Array(("info","id","Int")),Array(("info","id","2",">=")),hconf).foreach(r=>println(r.get(0)))
  val tbl_nm="xsb1"
  val show_col=Array(("info","id","Int"))
  hconf.set(TableInputFormat.INPUT_TABLE, tbl_nm)
  val table = new HTable(hconf, tbl_nm)
  val scan = new Scan()
  scan.setFilter(null)
  val ColumnValueScanner = table.getScanner(scan)
  var list_col: List[StructField] = List()
  list_col :+= StructField("rowkey", StringType, true)

  for (i <- show_col) {
    list_col :+= StructField(i._2, StringType, true)
  }
  //构建表的structType
  val schema = StructType(list_col)

  val tbl_rdd = ColumnValueScanner.iterator()
  //把过滤器加载到hbaseconf中
  hconf.set(TableInputFormat.SCAN, convertScanToString(scan))
  //构建RDD
  val hbaseRDD = sc.newAPIHadoopRDD(
    hconf,
    classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])
  //构建rdd的结果集
  val rowRDD = hbaseRDD.map { case (_, result) => {
    var valueSeq: Seq[String] = Seq()
    //获取行键
    val key = Bytes.toString(result.getRow)

    //通过列族和列名获取列  不加rowkey方法
    //      for(column <- columns) {
    //        valueSeq :+= Bytes.toString(result.getValue(family.getBytes, column.getBytes))
    //      }
    //加rowkey方法，Array第一列必须是"rowkey"
    valueSeq :+= key
    for (row <- show_col) {
      valueSeq :+= Bytes.toString(result.getValue(row._1.getBytes, row._2.getBytes))
    }
    org.apache.spark.sql.Row.fromSeq(valueSeq)
  }
  }.collect().foreach(println(_))

}
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

}
