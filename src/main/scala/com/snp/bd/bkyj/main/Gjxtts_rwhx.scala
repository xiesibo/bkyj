package com.snp.bd.bkyj.main

import java.io.{IOException, File}
import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

import com.snp.bd.bkyj.dataflow.LoginUtil
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Connection, Table, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

/**
  * 统计每个月每天相同轨迹天数最多
  * Created by x on 2018/6/30.
  */
object Gjxtts_rwhx {
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
/*    val userData = Array("20180103012945,18874021293,wt026vza",
      "20180103022945,18874021293,wt026vzb",
      "20180103032945,18874021293,wt026vzc",
      "20180103042945,18874021293,wt026vzd",
      "20180103052945,18874021293,wt026vze",
      "20180103062945,18874021293,wt026vzf",
      "20180104012945,18874021293,wt026vza",
      "20180104022945,18874021293,wt026vzb",
      "20180104032945,18874021293,wt026vzc",
      "20180104042945,18874021293,wt026vzd",
      "20180104052945,18874021293,wt026vze",
      "20180104062945,18874021293,wt026vzf",
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
    val cdrRDD = sc.parallelize(userData) //生成rdd
    val cdrRddROW = cdrRDD.map(row => {
        val split = row.split(",");
        org.apache.spark.sql.Row(split(1), split(0), split(2))
      })
    val cdrStruct = StructType(Array(
      StructField("usernum", StringType, true),
      StructField("begintime", StringType, true),
      StructField("geohash", StringType, true)
    ))
    sqlContext.createDataFrame(cdrRddROW,cdrStruct).registerTempTable("wj_dw_cdr")*/
    sqlContext.read.parquet("hdfs://hacluster/user/hive/warehouse/wj_dw_cdr").registerTempTable("wj_dw_cdr")
    val cdr = sqlContext.sql("select usernum,begintime,geohash from wj_dw_cdr")
    val gjRdd = cdr.rdd.map(row => (row.getString(0) + "@" + row.getString(1).substring(8, 10), Array((row.getString(1), row.getString(2))))).reduceByKey(_ ++ _).flatMap(v => {
      val listBf = new ListBuffer[Row]()
      val arr = v._2.sortWith((v1, v2) => {
        if (v1._1 < v2._1) true else false
      })
      var last = ""
      val str = new StringBuilder
      //去除轨迹中上下两个相同的记录 abccdd=>abcd
      arr.map(tv => tv._2).foreach(s => {
        if (!s.equals(last)) {
          last = s
          str.append(s).append("@")
        }
      })
      listBf.append(Row(v._1.split("@")(0), str.toString()))
      listBf.iterator
    })
    val userStruct = StructType(Array(
      StructField("phone", StringType, true),
      StructField("lx", StringType, true)
    ))
    val gjdf = sqlContext.createDataFrame(gjRdd, userStruct)
    gjdf.registerTempTable("t")

    sqlContext.sql("select phone,max(xtlx) m from (select phone,count(1) as xtlx from t group by phone,lx) lxt group by phone").rdd.
      foreachPartition(iterator => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd");
      val calendar = Calendar.getInstance();
      val day = sdf.format(calendar.getTime) + "~" + sdf.format(new Date())
      val putList = new java.util.ArrayList[Put]()
      for (row <- iterator) {
        val put = new Put(Bytes.toBytes(row.getString(0)))
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("fxsd"), Bytes.toBytes(day))
        if (row.get(1) != null) {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("gjxtts"), Bytes.toBytes(row.get(1).toString))
        } else {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("gjxtts"), null)
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
