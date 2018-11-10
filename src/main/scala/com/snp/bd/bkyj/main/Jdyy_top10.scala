package com.snp.bd.bkyj.main

import java.io.File
import java.sql.Date
import java.util.UUID

import com.snp.bd.bkyj.dataflow.LoginUtil
import com.snp.bd.bkyj.rule.{DBFactory, RuleService}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Scan, HTable}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType, StringType, StructField}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

/**
  * 禁毒应用
  * Created by x on 2018/7/4.
  */
object Jdyy_top10 {
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
  def main(args: Array[String]) {
    val columnFamily = "f"
    val tableName = "rwhx:rwhx"

    //获取所有禁毒案件id
    val anJianList = RuleService.getInstance.getJdAj
    val sparkConf = new SparkConf().setAppName("Pzfx").set("spark.inputFormat.cache.enabled", "false").set("spark.hbase.obtainToken.enabled", "true")
    sparkConf.setMaster("local[2]")
    //sparkConf.setMaster("yarn-client")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val map = Map("url" -> DBFactory.url, "dbtable" -> "WJ_SUPECT", "driver" -> DBFactory.driver, "user" -> DBFactory.username, "password" -> DBFactory.password)
    val WJ_SUPECT=sqlContext.read.options(map).format("jdbc").load.filter("DATATYPE=1").select("BUSINESSID","DATAVALUE")
    WJ_SUPECT.cache()
    val rkxx=getTableNm("rkxx_table", Array(("f1", "xm", "String"), ("f1", "lxdh", "String"), ("f1", "sfzh", "String"), ("f1", "xb", "String")), Array(), hconf,sc,sqlContext).select("lxdh","xm","sfzh", "xb")
    val vertices=rkxx.map(r=>(r.getLong(0),r.getString(1)))
    sqlContext.read.parquet("hdfs://hacluster/user/hive/warehouse/wj_dw_cdr").registerTempTable("wj_dw_cdr")
    sqlContext.read.parquet("hdfs://hacluster/user/hive/warehouse/jd").registerTempTable("jd")
    val jd= sqlContext.sql("select shprmobile,cneemobile from jd ")
    val cdr = sqlContext.sql("select usernum,relatenum from wj_dw_cdr ")
    import scala.collection.JavaConverters._
    //获取通话记录中含有涉毒人员的记录
    anJianList.asScala.foreach(v=>{
      val supects=sc.broadcast(WJ_SUPECT.filter(s"BUSINESSID=$v").map(row=>row.getAs[String]("DATAVALUE")).collect())
      getTop10(vertices, cdr, v,1)//cdr
      getTop10(vertices, jd, v,2)//cdr
      def getTop10(vertices: RDD[(Long, String)], cdr: DataFrame, v: Integer,dataType:Int): Unit = {
        val sdryjl = cdr.mapPartitions(itr => {
          val list = new ListBuffer[(Long, Long)]()
          val sarr = supects.value
          while (itr.hasNext) {
            val t = itr.next()
            if (sarr.contains(t.getString(0)) || sarr.contains(t.getString(1))) {
              list.append((t.getString(0).toLong, t.getString(1).toLong))
            }
          }
          list.iterator
        }).map(r => Edge(r._1, r._2, 0))
        val graph = Graph(vertices, sdryjl, "")
        val res = graph.inDegrees.fullOuterJoin(graph.outDegrees).map(t => {
          (t._1, t._2._1.getOrElse(0) + t._2._2.getOrElse(0))
        }).sortBy(f => f._2, false).top(10).map(v => v._1.toString).mkString(",")
        // 禁毒应用通话top10
        val sql = "INSERT INTO \"WJ_BIGDATA\".\"JDYY_TOP10\" (\"ID\", \"ANJIAN_ID\", \"DATA_TYPE\", \"VALUE\", \"ADD_TIMESTAMP\") VALUES (?,?,?,?,?);"
        val conn = DBFactory.getInstance.getConnection;
        val ps = conn.prepareStatement(sql)
        ps.setString(1, UUID.randomUUID().toString)
        ps.setString(2, v.toString)
        ps.setInt(3, dataType)
        ps.setString(4, res)
        ps.setDate(5, new Date(System.currentTimeMillis()))
        ps.execute()
        conn.commit()
        ps.close()
        conn.close()
      }
    })
    //

  }



  /**
    *
    * @param tbl_nm     表名
    * @param show_col   _1 列族  _2列名  _3 列类型(String,Int,Double,Timestamp...)
    * @param filter_col _1 列族  _2列名  _3 筛选值  _4 筛选类型(=,<,>,!=...)
    * @return sqlcontext
    */
  def getTableNm(tbl_nm: String, show_col: Array[(String, String, String)], filter_col: Array[(String, String, String, String)], hbaseConf: Configuration,sc:SparkContext,sqlContext:SQLContext): DataFrame = {
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
}
