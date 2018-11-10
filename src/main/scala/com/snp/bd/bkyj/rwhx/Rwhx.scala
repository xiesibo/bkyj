package com.snp.bd.bkyj.rwhx

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.snp.bd.bkyj.hadoop.LoginUtil
import com.snp.bd.bkyj.hbase.HBaseClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/12/10.
  */
object Rwhx {

  val start = "2016-12-01 04:27:24"
  val end = "2016-12-31 04:27:24"

  def main(args: Array[String]): Unit = {

   // val Array(start_time,end_time) = args

    val userPrincipal = "nifi@HADOOP.COM"
    val userKeytabPath = "krb/user.keytab"
    val krb5ConfPath = "krb/krb5.conf"
    val hadoopConf: Configuration  = new Configuration()
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);

    val conf = new SparkConf().setAppName("hhh").setMaster("local[1]")
    val sc = new SparkContext(conf)

    //话单分析
    HdService.analyze(sc)

    //短信分析


    sc.stop()
  }

  /**
    *  日期转换
    * @param str
    * @return
    */
  def convert(str:String):Boolean={
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = sdf.parse(str)
    val ds = sdf.parse(start)
    val de = sdf.parse(end)
    return (d.getTime<=de.getTime && d.getTime>ds.getTime)
  }

}
