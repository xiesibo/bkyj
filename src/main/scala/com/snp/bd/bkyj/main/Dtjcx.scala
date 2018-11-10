package com.snp.bd.bkyj.main

import java.io.File

import java.io.{IOException, File, StringWriter, FileInputStream}
import java.net.{URI, URLDecoder}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.regex.Pattern
import java.util.{UUID, Properties, Calendar, Date}
import com.snp.bd.bkyj.dataflow.LoginUtil
import com.snp.bd.bkyj.model._
import com.snp.bd.bkyj.rule.{DBFactory, RuleCompareService}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import com.alibaba.fastjson.{JSON, JSONObject}
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
  * Created by x on 2018/6/23.
  */
object Dtjcx {
  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
/*  val userdir = System.getProperty("user.dir") + File.separator+"krb"+File.separator
  println(System.getProperty("user.dir"))
  val userPrincipal: String = "nifi@HADOOP.COM"
  val userKeytabPath: String = userdir+"user.keytab"
  val userKeyconfPath: String = userdir+"krb5.conf"
  val ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
  val ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
  val ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";
  val userName = "nifi"
  val hconf = HBaseConfiguration.create();
  LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabPath);
  LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
  LoginUtil.login(userName, userKeytabPath, userKeyconfPath, hconf)*/
val hconf = HBaseConfiguration.create();
  val sparkConf = new SparkConf().set("spark.inputFormat.cache.enabled", "false").set("spark.hbase.obtainToken.enabled", "true").set("spark.sql.authorization.enabled", "true").set("hive.security.authorization.enabled", "true")
  val sc = new SparkContext(sparkConf)
  val sqlContext=new SQLContext(sc)
  def main(args: Array[String]) {
    val task =URLDecoder.decode(args(0))//{"relation":"5","time":"10","id":"43122519960310009X","imsi":"460114512222030","data":[{"name":"网吧","typename":"wb","hash":"wgsd31","date":"2018-01-11T11:34:00Z"},{"name":"五一广场","typename":"dw","hash":"wgdge3","date":"2018-01-15T10:45:45Z"},{"name":"网吧","typename":"wb","hash":"wgsd31","date":"2018-01-15T11:34:00Z"},{"name":"网吧","typename":"wb","hash":"wgsd31","date":"2018-01-15T16:34:00Z"},{"name":"网吧","typename":"wb","hash":"wgsd31","date":"2018-01-15T17:11:00Z"}],"beginTime":"2018-01-01 00:00:00","endTime":"2018-01-25 00:00:00","target":"43122519960310009X","taskid":20595}
    val taskobj = JSON.parseObject(task, classOf[DtjcxTaskModel])
    val sqlContext = new SQLContext(sc)
    import org.apache.spark.sql._
    getTableNm("rkxx_table", Array(("f1", "xm", "String"), ("f1", "lxdh", "String"), ("f1", "sfzh", "String"), ("f1", "xb", "String")), Array(), hconf).select("xm", "lxdh", "sfzh", "xb").registerTempTable("rkxx")
    sqlContext.read.parquet("hdfs://hacluster/user/hive/warehouse/wj_dw_cdr").registerTempTable("wj_dw_cdr")
    val cdr = sqlContext.sql("select usernum as lxdh,latitude,longitude,begintime,relatenum,xm,xb,sfzh from wj_dw_cdr left join rkxx on rkxx.lxdh=wj_dw_cdr.usernum")
    val rkcdr = cdr.map(r=>Row(r.getAs[String]("lxdh")+"@"+r.getAs[String]("xm")+"@"+r.getAs[String]("sfzh")+"@"+r.getAs[String]("xb"),r.getAs[Float]("latitude"),r.getAs[Float]("longitude"),r.getAs[String]("begintime"),r.getAs[String]("relatenum")))
    rkcdr.mapPartitions(itr=>{
      val list = new ListBuffer[(String, Int)]()
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      while (itr.hasNext){
        val row =itr.next()
        val time=sdf.parse(row.getString(3)).getTime
        if(taskobj.getBeginDate.getTime<time&&time<taskobj.getEndDate.getTime){
          val sb = new StringBuilder()
          val areas=taskobj.getQuyuxuanxiang
          for (i <- 0 until areas.size()) {
            var flag = "0"
            //是否在区域内
            if (existErea(areas.get(i), row.getFloat(1), row.getFloat(2))) {
              //是否满足其他限定条件
              var sign=true
              import collection.JavaConversions._
              for(xw: Xingweitiaojian<-taskobj.getXingweitiaojian){
                for(t<-xw.getTiaojian.split(",") if(sign)){
                  t match {
                    case "1"=>{
                      if(!xw.getTiaojianxinxi.equals(row.getString(4))){
                        sign=false
                      }
                    }
                      //TODO 其他条件 上网
                  }
                }
              }
              for(qt: Qitatiaojian<-taskobj.getQitatiaojian){
                if(!qt.getSex.equals(row.getString(0).split("@")(3))){
                  sign=false
                }
                //年龄条件判断
                val cal = Calendar.getInstance
                val nl=cal.get(Calendar.YEAR)+1-row.getString(0).split("@")(2).substring(6, 10).toInt
                var nlsign=false
                for(t<-qt.getCaseage.split(",")){
                  if(t.equals("20~30")){
                    if(20<nl&&nl<30)nlsign=nlsign|true
                  }else if(t.equals("40~60")){
                    if(40<nl&&nl<60)nlsign=nlsign|true
                  }else if(t.equals("70以上")){
                    if(70<nl)nlsign=nlsign|true
                  }
                }
                if(!nlsign) sign=false
              }
              if(sign)flag="1"
            }

            sb.append(flag)
          }
          list.append((row.getString(0), Integer.parseInt(sb.toString(), 2)))
        }
      }
      list.iterator
    }).reduceByKey(_ & _).map(r => {
      var c = 0
      var n = r._2
      while (n != 0) {
        c += 1
        n = n & (n - 1)
      }
      (r._1, c)
    }).filter(r => r._2 ==taskobj.getQuyuxuanxiang.size()).foreachPartition(itr => {
      val conn = DBFactory.getInstance.getConnection;
      var count = 1
      val sql="INSERT INTO \"WJ_BIGDATA\".\"SNP_RESULT\" (\"RELATION\", \"TASKID\", \"MOBILE\", \"IDCARD\", \"USERNAME\",\"ID\") VALUES(?,?,?,?,?,?)"
      val ps=conn.prepareStatement(sql)
      while (itr.hasNext) {
        val row = itr.next()
        ps.setInt(1, row._2)
        ps.setString(2, taskobj.getTaskid.toString)
        ps.setString(3, row._1.split("@")(0))
        ps.setString(4, row._1.split("@")(2))
        ps.setString(5, row._1.split("@")(1))
        ps.setString(6,UUID.randomUUID().toString)
        ps.addBatch()
        if (count % 1000 == 0) {
          ps.executeBatch()
          conn.commit()
        }
      }
      ps.executeBatch()
      conn.commit()
      ps.close()
      conn.close()
    })

  }
  /**
    *
    * @param tbl_nm     表名
    * @param show_col   _1 列族  _2列名  _3 列类型(String,Int,Double,Timestamp...)
    * @param filter_col _1 列族  _2列名  _3 筛选值  _4 筛选类型(=,<,>,!=...)
    * @return sqlcontext
    */
  def getTableNm(tbl_nm: String, show_col: Array[(String, String, String)], filter_col: Array[(String, String, String, String)],hbaseConf:Configuration): DataFrame = {
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tbl_nm)
    val table = new HTable(hbaseConf, tbl_nm)
    val scan = new Scan()

    /**
      * 指定列族和需要显示的列名
      * 添加多个需要用到的列
      */
    /*
    val length = show_col.length
    for(i <- show_col){
      scan.addColumn(Bytes.toBytes(i._1),Bytes.toBytes(i._2))
    }
    */
    //设置rowkey的范围，启示和结束
    //scan.setStartRow(Bytes.toBytes(""))
    //scan.setStopRow(Bytes.toBytes(""))
    val fil_len = filter_col.length
    println("------>>>>" + fil_len)
    //如果没有添加过滤器，就给过滤器添加空
    if (fil_len > 0) {
      val filter_arr = new java.util.ArrayList[Filter](fil_len)

      for (i <- filter_col) {
        i._4 match {
          case "=" => {
            val filter1 = new SingleColumnValueFilter(Bytes.toBytes(i._1),
              Bytes.toBytes(i._2), CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(i._3)))
            filter1.setFilterIfMissing(true)
            filter_arr.add(filter1)
          }
          case "<" => {
            val filter1 = new SingleColumnValueFilter(Bytes.toBytes(i._1),
              Bytes.toBytes(i._2), CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes(i._3)))
            filter1.setFilterIfMissing(true)
            filter_arr.add(filter1)
          }
          case "<=" => {
            val filter1 = new SingleColumnValueFilter(Bytes.toBytes(i._1),
              Bytes.toBytes(i._2), CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(i._3)))
            filter1.setFilterIfMissing(true)
            filter_arr.add(filter1)
          }
          case ">" => {
            val filter1 = new SingleColumnValueFilter(Bytes.toBytes(i._1),
              Bytes.toBytes(i._2), CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(i._3)))
            filter1.setFilterIfMissing(true)
            filter_arr.add(filter1)
          }
          case ">=" => {
            val filter1 = new SingleColumnValueFilter(Bytes.toBytes(i._1),
              Bytes.toBytes(i._2), CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(i._3)))
            //filter1.setFilterIfMissing(true)
            filter_arr.add(filter1)
          }
          case "!=" => {
            val filter1 = new SingleColumnValueFilter(Bytes.toBytes(i._1),
              Bytes.toBytes(i._2), CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(i._3)))
            filter1.setFilterIfMissing(true)
            filter_arr.add(filter1)
          }
          case _ => {}
        }
      }
      /**
        * 通过使用filterlist可以加载多个过滤器
        * 设置多个过滤器
        */
      val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter_arr)
      scan.setFilter(filterList)
    } else {
      scan.setFilter(null)
    }

    //hbaseConf.set(TableInputFormat.SCAN,convertScanToString(scan))
    //获取表的扫描
    val ColumnValueScanner = table.getScanner(scan)
    //构建structtype需要的list  根据传入的类型参数构建表
    /*var list_col = show_col.map{x=>{
     /* x._3 match {
        case "String" => StructField(x._2,StringType,true)
        case "Int" => StructField(x._2,StringType,true)
        case "Double" => StructField(x._2,StringType,true)
        case "Timestamp" => StructField(x._2,StringType,true)
        case _ => StructField(x._2,StringType,true)
      }*/
      StructField(x._2,StringType,true)
    }
    }*/
    /**
      * structType构造的目的：为在后面产生dataframe的时候指定每个值的列名
      * 在注册成表的时候可以使用
      */
    var list_col: List[StructField] = List()
    list_col :+= StructField("rowkey", StringType, true)

    for (i <- show_col) {
      list_col :+= StructField(i._2, StringType, true)
    }


    //构建表的structType
    val schema = StructType(list_col)

    val tbl_rdd = ColumnValueScanner.iterator()
    //把过滤器加载到hbaseconf中
    hbaseConf.set(TableInputFormat.SCAN, convertScanToString(scan))
    //构建RDD
    val hbaseRDD = sc.newAPIHadoopRDD(
      hbaseConf,
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
    }
    val hbasedataframe = sqlContext.createDataFrame(rowRDD, schema)

    //hbasedataframe.registerTempTable(tbl_nm)
    hbasedataframe
  }
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }
  /**
    * 是否在范围内
    *
    * @param quyu
    * @return
    */
  def existErea(quyu: Quyuxuanxiang, lat: Float, lng: Float): Boolean = {
    val km = new KeyAreaModel()
    km.setArea(JSON.toJSONString(quyu, false))
    if (RuleCompareService.inOrOut(km, lat, lng) == 1) return true else false
  }
}
