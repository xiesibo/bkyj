package com.snp.bd.bkyj.rwhx

import com.snp.bd.bkyj.hbase.HBaseClient
import com.snp.bd.bkyj.rwhx.Rwhx._
import org.apache.spark.SparkContext

/**
  * Created by Administrator on 2017/12/15.
  */
object HdService {

  val table = "rwhx_table"
  val family = "f"

  /**
    * 分析话单数据
    * @param sc
    */
  def analyze(sc:SparkContext): Unit ={
    //读取话单数据
    val hd = sc.textFile("hdfs://hd3:25000/wxh/warehouse/hd/d")

    val hd_filter = hd.map(_.split(",")).filter(x=>nothead(x(0))).filter(x=>convert(x(1)))
    val hd_out = hd_filter.map(x=>(x(2),(1,x(17))))
    val hd_in = hd_filter.map(x=>(x(4),(1,x(17))))

    val h_out_count = hd_out.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x=>(x._1,(x._2._1,"O"+x._2._2)))
    val h_in_count = hd_in.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x=>(x._1,(x._2._1,"I"+x._2._2)))
    val h_count = h_out_count.union(h_in_count)
   val h_group = h_count.groupByKey()

    val hd_out_count = hd_out.countByKey().map(x=>(x._1,"O"+x._2))
    val  hd_in_count = hd_in.countByKey().map(x=>(x._1,"I"+x._2))

    val out = sc.parallelize(hd_out_count.toSeq)
    val in = sc.parallelize(hd_in_count.toSeq)

    val hd_count = in.union(out)

    val hd_group = hd_count.groupByKey()

    //hd_group.foreach(x=>compute(x._1,x._2))
  }

  /**
    * 计算通话记录
    * @param phone
    * @param v
    */
  def compute(phone:String,v:Iterable[(Int,String)]):Unit= {

    var o: Int = 0
    var i: Int = 0
    v.foreach(x => {


//      if (x.startsWith("O")) {
//        o = Integer.parseInt(x.substring(1))
//      }
//      else if (x.startsWith("I")) {
//        i = Integer.parseInt(x.substring(1))
//      }
      println("呼入电话：" + i)
      println("呼出电话：" + o)

      //c  电话接次数
      HBaseClient.put(table,phone,family,"c",i+"")
      //d  电话打次数
      HBaseClient.put(table,phone,family,"d",o+"")

    })
  }

  /**
    * 过滤掉数据头行
    * @param str
    * @return
    */
  def nothead(str:String):Boolean={
    if(str.contains("rowkey"))return false;
    return true;
  }

}
