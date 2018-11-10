package com.snp.bd.bkyj.rwhx

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import com.snp.bd.bkyj.hadoop.LoginUtil

object FemaleInfoCollection {
  def main (args: Array[String]) {

//    if (args.length < 1) {
//
//      System.err.println("Usage: CollectFemaleInfo <file>")
//
//      System.exit(1)
//
//    }

    val userPrincipal = "nifi"
    val userKeytabPath = "krb/user.keytab"
    val krb5ConfPath = "krb/krb5.conf"
    val hadoopConf: Configuration  = new Configuration()
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);

    // Configure the Spark application name.

    val conf = new SparkConf().setAppName("CollectFemaleInfo").setMaster("local[1]")

    // Initializing Spark

    val sc = new SparkContext(conf)

    // Read data. This code indicates the data path that the input parameter args(0) specifies.

    val text = sc.textFile("a.txt")

    // Filter the data information about the time that female netizens spend online.

    val data = text.filter(_.contains("female"))



    sc.stop()

  }

}
