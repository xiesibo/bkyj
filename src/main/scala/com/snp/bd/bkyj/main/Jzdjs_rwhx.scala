package com.snp.bd.bkyj.main

import java.io.{File, IOException}
import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

import com.snp.bd.bkyj.dataflow.LoginUtil
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Connection, Table, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

/**
  * 居住地计算
  * Created by x on 2018/6/30.
  */
object Jzdjs_rwhx {
  def main(args: Array[String]) {
    val columnFamily = "f"
    val tableName = "rwhx:rwhx"
    val hconf = HBaseConfiguration.create()

    val userdir = System.getProperty("user.dir") + File.separator + "src\\main\\resources" + File.separator;
    val userKeytabPath = userdir + "user.keytab"
    val krb5ConfPath = userdir + "krb5.conf"

    val userName = "nifi"
    val ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    val ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    val ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";
    LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabPath);
    LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
    LoginUtil.login(userName, userKeytabPath, krb5ConfPath, hconf)
    val sparkConf = new SparkConf().setAppName("Gjxtts").set("spark.inputFormat.cache.enabled", "false").set("spark.hbase.obtainToken.enabled", "true").set("spark.sql.authorization.enabled", "true").set("hive.security.authorization.enabled", "true")
    sparkConf.setMaster("local[2]")
    //sparkConf.setMaster("yarn-client")
    val sc = new SparkContext(sparkConf)
    //val sqlContext = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)

/*    val userData = Array("20180103112945,18874021293,wt026vza",
      "20180103122945,18874021293,wt026vzb",
      "20180103132945,18874021293,wt026vzc",
      "20180103142945,18874021293,wt026vzd",
      "20180103002945,18874021293,wt026vze",
      "20180103062945,18874021293,wt026vzf",
      "20180104112945,18874021293,wt026vza",
      "20180104122945,18874021293,wt026vzb",
      "20180104132945,18874021293,wt026vzc",
      "20180104142945,18874021293,wt026vzd",
      "20180104002945,18874021293,wt026vze",
      "20180104062945,18874021293,wt026vzf",
      "20180107112945,18874021293,wt026vza",
      "20180107122945,18874021293,wt026vzb",
      "20180107132945,18874021293,wt026vzc",
      "20180107142945,18874021293,wt026vzd",
      "20180107002945,18874021293,wt026vzf",
      "20180107062945,18874021293,wt026vzf",
      "20180105012945,18874021293,wt026vza",
      "20180105022945,18874021293,wt026vzb",
      "20180105032945,18874021293,wt026vzc",
      "20180105042945,18874021293,wt026vzd",
      "20180105052945,18874021293,wt026vze",
      "20180105062945,18874021293,wt026vzf",
      "20180106012945,18874021293,wt026vza",
      "20180106022945,18874021293,wt026vzb",
      "20180106032945,18874021293,wt026vzc",
      "20180106042945,18874021293,wt026vzd",
      "20180106052945,18874021293,wt026vzg",
      "20180106062945,18874021293,wt026vzf",
      "20180103012945,18874021292,wt026vza",
      "20180103022945,18874021292,wt026vzb",
      "20180103032945,18874021292,wt026vzc",
      "20180103042945,18874021292,wt026vzd",
      "20180103052945,18874021292,wt026vze",
      "20180103062945,18874021292,wt026vzf",
      "20180104012945,18874021292,wt026vza",
      "20180104022945,18874021292,wt026vzb",
      "20180104032945,18874021292,wt026vzc",
      "20180104042945,18874021292,wt026vzd",
      "20180104052945,18874021292,wt026vze",
      "20180104062945,18874021292,wt026vzf",
      "20180105012945,18874021292,wt026vza",
      "20180105022945,18874021292,wt026vzb",
      "20180105032945,18874021292,wt026vzc",
      "20180105042945,18874021292,wt026vzd",
      "20180105052945,18874021292,wt026vze",
      "20180105062945,18874021292,wt026vzf",
      "20180106012945,18874021292,wt026vza",
      "20180106022945,18874021292,wt026vzb",
      "20180106032945,18874021292,wt026vzc",
      "20180106042945,18874021292,wt026vzd",
      "20180106052945,18874021292,wt026vzg",
      "20180106062945,18874021292,wt026vzf"
    )
    val cdrRDD = sc.parallelize(userData)
    //生成rdd
    val cdrRddROW = cdrRDD.map(row => {
      val split = row.split(",");
      org.apache.spark.sql.Row(split(1), split(0), split(2))
    })
    val cdrStruct = StructType(Array(
      StructField("phone", StringType, true),
      StructField("times", StringType, true),
      StructField("geohash", StringType, true)
    ))
    sqlContext.createDataFrame(cdrRddROW, cdrStruct).registerTempTable("wj_dw_cdr")
    val cdr = sqlContext.sql("select phone,times,geohash from wj_dw_cdr")*/
    sqlContext.read.parquet("hdfs://hacluster/user/hive/warehouse/wj_dw_cdr").registerTempTable("wj_dw_cdr")
    val cdr = sqlContext.sql("select usernum,begintime,geohash from wj_dw_cdr")
    val gjRdd = cdr.rdd.filter(row => {
      val h = row.getString(1).substring(11, 13)
      if ((0 <= h.toInt && h.toInt <= 6) || (19 <= h.toInt && h.toInt <= 23)) true else false
    }).map(row => (row.getString(0) + "@" + row.getString(1).substring(8, 10), Array((row.getString(1), row.getString(2))))).reduceByKey(_ ++ _).flatMap(v => {
      val listBf = new ListBuffer[Row]()
      val arr = v._2.sortWith((v1, v2) => {
        if (v1._1 < v2._1) true else false
      })
      var maxgeo = ""
      var last = ""
      var lasths = ""
      var h = 0
      for(i<-0 until arr.length-1){
        var s=arr(i)
        if (!s._2.equals(last)) {
          if (lasths.length > 0) {
            var subt = s._1.substring(11, 13).toInt - lasths.toInt
            if(subt>13)subt=subt-13
            if (subt > h) {
              h = subt
              maxgeo = last
            }
          }
          lasths = s._1.substring(11, 13)
          last = s._2
        }
        if(i==arr.length-1&&lasths.toInt<23){
          val subt = 24 - lasths.toInt
          if (subt > h) {
            h = subt
            maxgeo = last
          }
        }
      }
      if (h > 4) {
        listBf.append(Row(v._1.split("@")(0), maxgeo))
      }
      listBf.iterator
    })
    val userStruct = StructType(Array(
      StructField("phone", StringType, true),
      StructField("jzd", StringType, true)
    ))
    val gjdf = sqlContext.createDataFrame(gjRdd, userStruct)
    gjdf.registerTempTable("t")
    val sdf = new SimpleDateFormat("yyyy-MM-dd");
    val calendar = Calendar.getInstance();
    val day = sdf.format(calendar.getTime) + "~" + sdf.format(new Date())
    sqlContext.sql("select phone,jzd,count(xtdz) m from (select phone,jzd,count(1) as xtdz from t group by phone,jzd) lx group by phone,jzd")
        .rdd.filter(r=>r.getInt(2)>15).map(r => (r.getString(0), Array(r.getString(1)+"-"+r.getInt(2)))).reduceByKey(_ ++ _).foreachPartition(iterator => {
        val putList = new java.util.ArrayList[Put]()
        for (row <- iterator) {
          val put = new Put(Bytes.toBytes(row._1))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("fxsd"), Bytes.toBytes(day))
          if (row._2 != null) {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("czd"), Bytes.toBytes(row._2.mkString(",")))
          } else {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("czd"), null)
          }
          putList.add(put)
        }
        var table: Table = null
        var connection: Connection = null
        connection = ConnectionFactory.createConnection(HBaseConfiguration.create())
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
  }

}
