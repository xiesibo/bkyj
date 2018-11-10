package com.snp.bd.bkyj.main

import java.io.{IOException, File}

import com.snp.bd.bkyj.dataflow.LoginUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by x on 2018/7/21.
  */
object Tlgx_rwhx {
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
    sqlContext.read.parquet("hdfs://hacluster/user/hive/warehouse/wj_dw_cdr").registerTempTable("wj_dw_cdr")
    val cdr = sqlContext.sql("select usernum,begintime,relatenum,callduration from wj_dw_cdr").filter("callduration>0")
    cdr.schema.foreach(s => println(s.name + s.dataType))
    cdr.rdd.map(row => {
      var sum=0.0
      //根据通话时间初始化权重
      var weight = 1.0
      val h = row.getString(1).substring(11, 13).toInt
      if (0 < h && h < 6) {
        weight = 1.2
      } else if (6 < h && h < 20) {
        weight = 1.0
      } else {
        weight = 1.1
      }

      var calldura = 0//s
      if (calldura % 60 == 0) {
        calldura = row.getInt(3) / 60
      } else {
        calldura = row.getInt(3) / 60 + 1
      }
      if(calldura==1){
        sum=weight;
      }else{
        sum=x(calldura/10,calldura%10,weight)
      }
      (row.getString(0)+"@"+row.getString(2),sum)
    }).reduceByKey(_+_).map(r=>(r._1.split("@")(0),Array((r._1.split("@")(1),r._2)))).reduceByKey(_++_).foreachPartition(itr=>{
      var table: Table = null
      var connection: Connection = null
      connection = ConnectionFactory.createConnection(HBaseConfiguration.create())
      table = connection.getTable(TableName.valueOf(tableName))
      val putList = new java.util.ArrayList[Put]()
      while (itr.hasNext){
        val row=itr.next()
        val put = new Put(Bytes.toBytes(row._1))
        if (row._2 != null&&row._2.length>0) {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("gxwl_tlgx"), Bytes.toBytes(row._2.map(t=>t._1+","+t._2).mkString("->")))
        } else {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("gxwl_tlgx"), null)
        }
        putList.add(put)
      }
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
  def x(d: Int, y: Int, w: Double): Double = {
    if (d > 0) {
      return x(d - 1, 10, w) + y * (1 + 0.1 * (d+1)) * w
    } else {
      if (y > 1) {
        return (y - 1) * 1.1 * w + w
      } else {
        return w
      }
    }
  }

}
