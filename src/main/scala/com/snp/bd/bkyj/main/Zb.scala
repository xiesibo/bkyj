package com.snp.bd.bkyj.main

import java.io.{IOException, File}
import java.text.SimpleDateFormat
import java.util.{Properties, Calendar, Date}

import com.snp.bd.bkyj.dataflow.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo

/**
  * 统计各类离线指标
  * Created by x on 2018/6/7.
  */
object Zb {
//  val userdir = System.getProperty("user.dir") + File.separator+"krb"+File.separator
//  println(System.getProperty("user.dir"))
//  val userPrincipal: String = "nifi@HADOOP.COM"
//  val userKeytabPath: String = userdir+"user.keytab"
//  val userKeyconfPath: String = userdir+"krb5.conf"
//  val ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
//  val ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
//  val ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";
//  val userName = "nifi"
  val hconf = HBaseConfiguration.create();
//  LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabPath);
//  LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
//  LoginUtil.login(userName, userKeytabPath, userKeyconfPath, hconf)
  //val userKeytabPath: String = "/user.keytab"
  //  val userKeyconfPath: String = "/krb5.conf"
  //  val userPrincipal: String = "nifi@HADOOP.COM"
  //  LoginUtil.login(userPrincipal, userKeytabPath, userKeyconfPath, new Configuration)


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zb").set("spark.inputFormat.cache.enabled", "false").set("spark.hbase.obtainToken.enabled", "true").set("spark.sql.authorization.enabled", "true").set("hive.security.authorization.enabled", "true")
    sparkConf.setMaster("yarn-client")
    val sc = new SparkContext(sparkConf)
    val sdf = new SimpleDateFormat("yyyy-MM-dd");
    val calendar=Calendar.getInstance();
    calendar.setTime(new Date())
    calendar.set(Calendar.MONTH,calendar.get(Calendar.MONTH)-1)
    val day=sdf.format(calendar.getTime)+"~"+sdf.format(new Date())
    val everyDayStartTime = 0
    val everyDayEndTime = 4
    val columnFamily = "f"
    val tableName = "rwhx:rwhx"
//    val props=new Properties()
//    props.setProperty("spark.sql.authorization.enabled", "true")
//    props.setProperty("hive.security.authorization.enabled", "true")
    sc.setLocalProperty("spark.sql.authorization.enabled", "true")
    sc.setLocalProperty("hive.security.authorization.enabled", "true")
    val hiveContext = new HiveContext(sc)
    //hiveContext.sql("create table test(id string)").show()
    val hddata = hiveContext.sql("select begintime,callduration,usernum,relatenum from wj_dw_cdr")
    hddata.show()
    hddata.registerTempTable("temp")
    val hdjtData = hiveContext.sql("select begintime,callduration,usernum as relatenum ,relatenum as usernum from temp")
    val hdallData = hddata.unionAll(hdjtData)
    hdallData.registerTempTable("hdallData")
    //每天通话时长，次数
    val mtthCsSc = hiveContext.sql("select usernum,sum(s),avg(c) mc,stddev(c)sc,avg(s) ms,stddev(s) ss from(select usernum,count(callduration) c,sum(callduration)/60.0 s ,tid from (select *,day(begintime) tid from hdallData) temp group by tid,usernum)t group by usernum")
    mtthCsSc.foreachPartition(iterator => {
      val putList = new java.util.ArrayList[Put]()
      for (row <- iterator) {
        val put = new Put(Bytes.toBytes(row.getString(0)))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("fxsd"), Bytes.toBytes(day))
        if (row.get(1) != null) {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("thsc"), Bytes.toBytes(row.get(1).toString))
        } else {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("thsc"), null)
        }
        if (row.get(2) != null) {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bk_d"), Bytes.toBytes(row.get(2).toString+","+row.get(3).toString))
        } else {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bk_d"), null)
        }
        if (row.get(4) != null) {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bk_a"), Bytes.toBytes(row.get(4).toString+","+row.get(5).toString))
        } else {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bk_a"), null)
        }
        putList.add(put)
      }
      var table: Table = null
      var connection: Connection = null
      connection = ConnectionFactory.createConnection(hconf)
      table = connection.getTable(TableName.valueOf(tableName))
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
    //每天与特定号码通话时长、次数
//    val mtthTdCsSc = hiveContext.sql("select usernum,relatenum,avg(c) mc,stddev(c)sc,avg(s) ms,stddev(s) ss from(select usernum,relatenum,count(callduration) c,sum(callduration)/60.0 s ,tid from (select *,day(begintime) tid from hdallData) temp group by tid,usernum,relatenum)t group by usernum,relatenum")
//    mtthTdCsSc.show()
    //每天0-4通话时长，次数
    val mtthCsSc0_4 = hiveContext.sql(s"select usernum,avg(c) mc,stddev(c)sc,avg(s) ms,stddev(s) ss from(select usernum,count(callduration) c,sum(callduration)/60.0 s ,tid from (select *,day(begintime) tid,hour(begintime)as h from hdallData) temp where h>=$everyDayStartTime and h<$everyDayEndTime group by tid,usernum)t group by usernum")
    mtthCsSc0_4.foreachPartition(iterator => {
      val putList = new java.util.ArrayList[Put]()
      for (row <- iterator) {
        val put = new Put(Bytes.toBytes(row.getString(0)))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("fxsd"), Bytes.toBytes(day))
        if (row.get(1) != null) {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bk_e"), Bytes.toBytes(row.get(1).toString+","+row.get(2).toString))
        } else {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bk_e"), null)
        }
        if (row.get(3) != null) {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bk_b"), Bytes.toBytes(row.get(3).toString+","+row.get(4).toString))
        } else {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bk_b"), null)
        }
        putList.add(put)
      }
      var table: Table = null
      var connection: Connection = null
      connection = ConnectionFactory.createConnection(hconf)
      table = connection.getTable(TableName.valueOf(tableName))
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
    //每天与特定号码通话时长、次数
    val mtthTdCsSc0_4 = hiveContext.sql(s"select usernum,relatenum,avg(c) mc,stddev(c)sc,avg(s) ms,stddev(s) ss from(select usernum,relatenum,count(callduration) c,sum(callduration)/60.0 s ,tid from (select *,day(begintime) tid,hour(begintime)as h from hdallData) temp where  h>=$everyDayStartTime and h<$everyDayEndTime group by tid,usernum,relatenum)t group by usernum,relatenum")
    mtthTdCsSc0_4.foreachPartition(iterator => {
      val putList = new java.util.ArrayList[Put]()
      for (row <- iterator) {
        val put = new Put(Bytes.toBytes(row.getString(0)))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("fxsd"), Bytes.toBytes(day))
        if (row.get(1) != null) {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bk_f"), Bytes.toBytes(row.get(1).toString+","+row.get(2).toString+","+row.get(3).toString))
        } else {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bk_f"), null)
        }
        if (row.get(3) != null) {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bk_c"), Bytes.toBytes(row.get(1).toString+","+row.get(4).toString+","+row.get(5).toString))
        } else {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bk_c"), null)
        }
        putList.add(put)
      }
      var table: Table = null
      var connection: Connection = null
      connection = ConnectionFactory.createConnection(hconf)
      table = connection.getTable(TableName.valueOf(tableName))
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
    //一个月拨打电话次数与时长
    val yBdmtthCsSc = hiveContext.sql("select count(callduration)c,usernum from wj_dw_cdr group by usernum")
    yBdmtthCsSc.foreachPartition(iterator => {
      val putList = new java.util.ArrayList[Put]()
      for (row <- iterator) {
        val put = new Put(Bytes.toBytes(row.getString(1)))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("fxsd"), Bytes.toBytes(day))
        if (row.get(1) != null) {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bddhs"), Bytes.toBytes(row.get(0).toString))
        } else {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bddhs"), null)
        }
        putList.add(put)
      }
      var table: Table = null
      var connection: Connection = null
      connection = ConnectionFactory.createConnection(hconf)
      table = connection.getTable(TableName.valueOf(tableName))
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
    //接听电话次数与时长
    val yJtmtthCsSc = hiveContext.sql("select count(callduration)c,relatenum from wj_dw_cdr group by relatenum")
    yJtmtthCsSc.foreachPartition(iterator => {
      val putList = new java.util.ArrayList[Put]()
      for (row <- iterator) {
        val put = new Put(Bytes.toBytes(row.getString(1)))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("fxsd"), Bytes.toBytes(day))
        if (row.get(1) != null) {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("jtdhs"), Bytes.toBytes(row.get(0).toString))
        } else {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("jtdhs"), null)
        }
        putList.add(put)
      }
      var table: Table = null
      var connection: Connection = null
      connection = ConnectionFactory.createConnection(hconf)
      table = connection.getTable(TableName.valueOf(tableName))
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
    //总通话次数与时长
    //val allThCsSc = hiveContext.sql("select count(callduration)c,sum(callduration)/60.0 s,usernum from hdallData group by usernum")
    //联系人个数
    val lxrgs = hiveContext.sql("select usernum ,count(distinct relatenum) renshu from hdallData group by usernum")
    lxrgs.foreachPartition(iterator => {
        val putList = new java.util.ArrayList[Put]()
        for (row <- iterator) {
          val put = new Put(Bytes.toBytes(row.getString(0)))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("fxsd"), Bytes.toBytes(day))
          if (row.get(1) != null) {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("lxrs"), Bytes.toBytes(row.get(1).toString))
          } else {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("lxrs"), null)
          }
          putList.add(put)
        }
        var table: Table = null
        var connection: Connection = null
        connection = ConnectionFactory.createConnection(hconf)
        table = connection.getTable(TableName.valueOf(tableName))
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
    /*    val hbConf: Configuration = HBaseConfiguration.create(sc.hadoopConfiguration)
        val scan = new Scan()
        scan.addFamily(Bytes.toBytes("f"))
        val proto = ProtobufUtil.toScan(scan)
        val scanToString = Base64.encodeBytes(proto.toByteArray)
        hbConf.set(TableInputFormat.INPUT_TABLE, "rwhx:rwhx")
        hbConf.set(TableInputFormat.SCAN, scanToString)
      println("****99")
        //  Obtain the data in the table through the Spark interface.
        val rdd = sc.newAPIHadoopRDD(hbConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
        // Traverse every row in the HBase table and print the results
        println("****103")
        rdd.collect().foreach(x => {
          val key = x._1.toString
          val it = x._2.listCells().iterator()
          while (it.hasNext) {
            val c = it.next()
            val family = Bytes.toString(CellUtil.cloneFamily(c))
            val qualifier = Bytes.toString(CellUtil.cloneQualifier(c))
            val value = Bytes.toString(CellUtil.cloneValue(c))
            val tm = c.getTimestamp
            println(" Family=" + family + " Qualifier=" + qualifier + " Value=" + value + " TimeStamp=" + tm)
          }
        })*/
    println("*****163")
    //getTableNm("xsb1",Array(("info","id","Int")),Array(("info","id","2",">=")),hconf).foreach(r=>println(r.get(0)))
    val tbl_nm = "rwhx:rwhx"
    val show_col = Array(("f", "id", "Int"))
    hconf.set(TableInputFormat.INPUT_TABLE, tbl_nm)
    val table = new HTable(hconf, tbl_nm)
    val scan = new Scan()
    scan.setFilter(null)
    val ColumnValueScanner = table.getScanner(scan)
    var list_col: List[StructField] = List()
    list_col :+= StructField("ROW", StringType, true)

    //    for (i <- show_col) {
    //      list_col :+= StructField(i._2, StringType, true)
    //    }
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

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable])
    kryo.register(classOf[org.apache.hadoop.hbase.client.Result])
    kryo.register(classOf[Array[(Any, Any)]])
    kryo.register(classOf[Array[org.apache.hadoop.hbase.Cell]])
    kryo.register(classOf[org.apache.hadoop.hbase.NoTagsKeyValue])
    kryo.register(classOf[org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionLoadStats])
  }
}