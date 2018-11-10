package com.snp.bd.bkyj.main

import com.snp.bd.bkyj.dataflow.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by x on 2018/5/12.
  */
object TestSpark {
  def main(args: Array[String]) {
    val userPrincipal: String = "nifi@HADOOP.COM"
    val userKeytabPath: String = "krb/user.keytab"
    val userKeyconfPath: String = "krb/krb5.conf"
    LoginUtil.login(userPrincipal, userKeytabPath, userKeyconfPath, new Configuration)
    val sparkConf = new SparkConf().setAppName("bkyj").setMaster("local[5]")
    val sqlContext=new org.apache.spark.sql.SQLContext(new SparkContext(sparkConf))
    sqlContext.read.format("jdbc").option("url", "jdbc:mysql://127.0.0.1:3306/test?createDatabaseIfNotExist=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;autoReconnect=true&amp;autoReconnectForPools=true").option("dbtable", "user").option("driver", "com.mysql.jdbc.Driver").option("password", "123456").option("user", "root").load().show()
  }
}
