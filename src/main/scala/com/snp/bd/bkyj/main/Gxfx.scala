package com.snp.bd.bkyj.main

import java.io.{IOException, File}
import java.net.{URLDecoder, URLEncoder}

import com.alibaba.fastjson.JSON
import com.snp.bd.bkyj.dataflow.LoginUtil
import com.snp.bd.bkyj.model.{GxfxModel, PzfxTaskModel, QuyuModel}
import com.snp.bd.bkyj.util.GraphNdegUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.graphx.{EdgeDirection, VertexId, Graph, Edge}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StructType, StringType, StructField}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

/**
  * Created by x on 2018/6/26.
  */
object Gxfx {
  val columnFamily = "f"
  val tableName = "rwhx:relation"
  val hconf = HBaseConfiguration.create()

  val log = org.apache.log4j.LogManager.getLogger(Gxfx.getClass)
  val userdir = System.getProperty("user.dir") + File.separator + "src\\main\\resources" + File.separator;
  val userKeytabPath = userdir + "user.keytab"
  val krb5ConfPath = userdir + "krb5.conf"
//val userKeytabPath: String = "/user.keytab"
//  val krb5ConfPath: String = "/krb5.conf"
/*  val userName = "nifi"
  val ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
  val ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
  val ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";
  LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabPath);
  LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
  LoginUtil.login(userName, userKeytabPath, krb5ConfPath, hconf)*/
  def main(args: Array[String]) {
    val hconf = HBaseConfiguration.create();
    val sparkConf = new SparkConf()
    //sparkConf.setMaster("local").setAppName("tt")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import scala.collection.JavaConverters._
    var relation = 0
    val task =URLDecoder.decode(args(0))//"{\"beginDate\":\"2016-12-12 00:00:00\",\"endDate\":\"2018-06-19 00:00:00\",\"relation\":3,\"query\":\"13007336756\",\"relationPerson\":\"13397485383\"}"
    println("****"+task)
    val taskobj = JSON.parseObject(task, classOf[GxfxModel])
    relation=taskobj.getRelation
    import org.apache.spark.sql._
    val beginDate=taskobj.getBeginDate
    val endDate=taskobj.getEndDate
    getTableNm("rkxx_table", Array(("f1", "xm", "String"), ("f1", "lxdh", "String"), ("f1", "sfzh", "String"), ("f1", "xb", "String")), Array(), hconf,sc,sqlContext).select("xm", "lxdh", "sfzh", "xb").registerTempTable("rkxx")
    sqlContext.read.parquet("hdfs://hacluster/user/hive/warehouse/wj_dw_cdr").registerTempTable("wj_dw_cdr")
    val cdr = sqlContext.sql(s"select usernum as lxdh,relatenum from wj_dw_cdr where begintime>='$beginDate' and begintime<='$endDate'")
    cdr.show()
    val rkcdr=cdr.map(row=>(row.getString(0).toLong,row.getString(1).toLong)).union(cdr.map(row=>(row.getString(1).toLong,row.getString(0).toLong)))
    //查询两个人的关系
    if(taskobj.getRelationPerson!=null&&taskobj.getRelationPerson.length>0){
      val arr=rkcdr.filter(r=>r._1.toString().equals(taskobj.getQuery)).take(1)
        val arr_relate=rkcdr.filter(r=>r._1.toString().equals(taskobj.getQuery)).take(1)
      if(arr.size>0&&arr_relate.size>0){
        val userInfo = Array("1,2"
        )
        val vertices = sc.parallelize(userInfo).map(r => {
          val fields = r.split(",")
          (fields(0).toLong, fields(1))
        })
        val edges = rkcdr.map(r => {
          Edge(r._1, r._2, 0)
        })
        val graph = Graph(vertices, edges, "").persist()

        val sourceId: VertexId = taskobj.getQuery.toLong // The ultimate source

        // Initialize the graph such that all vertices except the root have distance infinity.
        val initialGraph : Graph[(Double, List[VertexId]), Int] = graph.mapVertices((id, _) =>
          if (id == sourceId) (0.0, List[VertexId](sourceId))
          else (Double.PositiveInfinity, List[VertexId]()))

        val sssp = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()), relation, EdgeDirection.Out)(

          // Vertex Program节点处理消息的函数，dist为原节点属性（Double），newDist为消息类型（Double）
          (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,

          // Send Message发送消息函数，返回结果为（目标节点id，消息（即最短距离））
          triplet => {
            if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr ) {
              Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr , triplet.srcAttr._2 :+ triplet.dstId)))
            } else {
              Iterator.empty
            }
          },
          //Merge Message
          (a, b) => {if (a._1 < b._1) a else b})
        sssp.vertices.foreach(println(_))
        val relate =sssp.vertices.filter(v=>v._1==taskobj.getRelationPerson.toLong&&v._2._2.size>0).map(v=>(v._1,v._2._2)).take(1)
        println(relate.head._2.mkString(","))
        if(relate.length>0){
          // 保存两人关系
          var table: Table = null
          var connection: Connection = null
          connection = ConnectionFactory.createConnection(hconf)
          table = connection.getTable(TableName.valueOf(tableName))
          val put = new Put(Bytes.toBytes(sc.appName+"+"+relate.head._1))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("rel"), Bytes.toBytes(relate.head._2.mkString(",")))
          table.put(put)
          if (connection != null) {
            try {
              //关闭Hbase连接.
              connection.close()
            } catch {
              case e: IOException =>
                e.printStackTrace()
            }
          }
        }
        /*    val root=arr.head.getString(0).split("@")(0)
            val relate=arr_relate.head.getString(0).split("@")(0)
            val relationships=rkcdr.filter(r=>r.getString(0).split("@")(0).length==11&&r.getString(4).length==11).map(r=>(r.getString(0).split("@")(0).toLong,r.getString(4).toLong))
            val relaterdd=GraphNdegUtil.aggNdegreedVertices(relationships,sc.parallelize(Array(root.toLong)),3).flatMap(r=>{
              val list=new ListBuffer[Row]()
              r._2.foreach(t=>t._2.foreach(t1=>list.append(Row(r._1,t._1,t1))))
              list.iterator
            })
            val relateStruct = StructType(Array(
              StructField("lxdh", StringType, true),
              StructField("relation", IntegerType, true),
              StructField("tarnum", StringType, true)
            ))
            val relateDf=sqlContext.createDataFrame(relaterdd,relateStruct)*/

      }else{
        log.info("*** user not exist!")
      }
    }else{
      //查询一个人的关系
      val arr=rkcdr.filter(r=>r._1.toString().equals(taskobj.getQuery)).take(1)
      if(arr.size>0){
        var table: Table = null
        var connection: Connection = null
        connection = ConnectionFactory.createConnection(hconf)
        table = connection.getTable(TableName.valueOf(tableName))
          rkcdr.map(r=>(r._1,Array(r._2.toString))).reduceByKey(_++_).foreachPartition(itr=> {
            var table: Table = null
            var connection: Connection = null
            connection = ConnectionFactory.createConnection(hconf)
            table = connection.getTable(TableName.valueOf(tableName))
            val putList = new java.util.ArrayList[Put]()
            while (itr.hasNext){
              val row=itr.next()
              val put = new Put(Bytes.toBytes(sc.appName+"+"+row._1))
              put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("rel"), Bytes.toBytes(row._2.mkString(",")))
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
        //关系分析结果存储

      }else{
        log.info("*** user not exist!")
      }
    }

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
