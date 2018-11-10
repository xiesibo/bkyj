package com.snp.bd.bkyj.main

import java.io.File

import com.snp.bd.bkyj.dataflow.LoginUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by x on 2018/6/23.
  */
object TestJoin {
  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val userdir = System.getProperty("user.dir") + File.separator+"krb"+File.separator
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
  LoginUtil.login(userName, userKeytabPath, userKeyconfPath, hconf)
  val sparkConf = new SparkConf().setAppName("zb").set("spark.inputFormat.cache.enabled", "false").set("spark.hbase.obtainToken.enabled", "true").set("spark.sql.authorization.enabled", "true").set("hive.security.authorization.enabled", "true")
  sparkConf.setMaster("local")
  val sc = new SparkContext(sparkConf)
  val sqlContext=new SQLContext(sc)
  def main(args: Array[String]) {
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
    val df1=sqlContext.createDataFrame(userRddROW,userStruct)
    val df2=df1.filter("id>3").selectExpr("id","time")
    df1.join(df2,Seq("id")).map(r=>r.getAs[String]("url")).foreach(println(_))
    df1.selectExpr("id","time").rdd.union(df2.rdd).foreach(println(_))
  }
}
