package com.snp.bd.bkyj.main

import java.io.File
import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util.UUID

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.snp.bd.bkyj.model.{KeyAreaModel, QuyuModel, PzfxTaskModel}
import com.snp.bd.bkyj.rule.{DBFactory, RuleCompareService}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Scan, HTable}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.types.{StructType, IntegerType, StructField, StringType}
import org.apache.spark.{SparkContext, SparkConf}
import com.snp.bd.bkyj.dataflow.LoginUtil

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONObject

/**
  * Created by x on 2018/6/19.
  */
object Pzfx {
/*  val userdir = System.getProperty("user.dir") + File.separator + "src\\main\\resources" + File.separator;
  val userKeytabPath = userdir+"user.keytab"
  val krb5ConfPath = userdir+"krb5.conf"

  val userName = "nifi"
  val hconf = HBaseConfiguration.create();
  val ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
  val ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
  val ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";
  LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabPath);
  LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
  LoginUtil.login(userName, userKeytabPath, krb5ConfPath, hconf)*/
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().set("spark.inputFormat.cache.enabled", "false").set("spark.hbase.obtainToken.enabled", "true")
    //sparkConf.setMaster("local[2]")
    //sparkConf.setMaster("yarn-client")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
  val hconf = HBaseConfiguration.create();
    import scala.collection.JavaConverters._
    var relation = 0
    var quyus: java.util.List[QuyuModel] = null//
  val task =URLDecoder.decode(args(0))//"{\"relation\":\"1\",\"data\":[{\"interval\":\"5\",\"range\":98957,\"grohash\":\"wmnhq2uk\",\"date\":\"2016-12-12 20:19:10\",\"lower_left\":\"[113.94092,111.94092]\",\"upper_right\":\"[27.03622,25.03622]\",\"type\":\"Polygon\"},{\"interval\":\"5\",\"range\":\"500\",\"grohash\":\"wmpx29vm\",\"date\":\"2018-06-20 21:12:51\",\"type\":\"Circle\",\"radius\":92658.1306091436,\"point\":\"[111.826399,29.410044]\"}],\"taskid\":22754}"
    val taskobj = JSON.parseObject(task, classOf[PzfxTaskModel]).asInstanceOf[PzfxTaskModel]
    relation = taskobj.getRelation.toInt
    quyus = taskobj.getData
    println("**" + quyus.size())
    import org.apache.spark.sql._
    getTableNm("rkxx_table", Array(("f1", "xm", "String"), ("f1", "lxdh", "String"), ("f1", "sfzh", "String"), ("f1", "xb", "String")), Array(), hconf,sc,sqlContext).select("xm", "lxdh", "sfzh", "xb").registerTempTable("rkxx")
    sqlContext.read.parquet("hdfs://hacluster/user/hive/warehouse/wj_dw_cdr").registerTempTable("wj_dw_cdr")
  sqlContext.read.parquet("hdfs://hacluster/user/hive/warehouse/wj_net_regist").registerTempTable("wj_net_regist")
  val cdr = sqlContext.sql("select usernum as lxdh,latitude,longitude,begintime,xm,xb,sfzh from wj_dw_cdr left join rkxx on rkxx.lxdh=wj_dw_cdr.usernum")
    val rkcdr = cdr.map(r=>Row(r.getAs[String]("lxdh")+"@"+r.getAs[String]("xm")+"@"+r.getAs[String]("sfzh")+"@"+r.getAs[String]("xb"),r.getAs[Float]("latitude"),r.getAs[Float]("longitude"),r.getAs[String]("begintime")))
    val map = Map("url" -> DBFactory.url, "dbtable" -> "WJ_NET", "driver" -> DBFactory.driver, "user" ->DBFactory.username, "password" -> DBFactory.password)
    val WJ_NET=sqlContext.read.options(map).format("jdbc").load
    WJ_NET.registerTempTable("WJ_NET")
    val wb=sqlContext.sql("select lxdh,xm,xb,id_card as sfzh,open_time as begintime,WJ_NET.LATITUDE as latitude,WJ_NET.LONGITUDE as longitude from wj_net_regist LEFT JOIN WJ_NET on wj_net_regist.net_code=WJ_NET.ID LEFT JOIN rkxx on rkxx.sfzh=wj_net_regist.id_card")
    val wbxx=wb.map(r=>Row(r.getAs[String]("lxdh")+"@"+r.getAs[String]("xm")+"@"+r.getAs[String]("sfzh")+"@"+r.getAs[String]("xb"),r.getAs[Float]("latitude"),r.getAs[Float]("longitude"),r.getAs[String]("begintime")))
    val userRddROW = rkcdr.union(wbxx)
    userRddROW.cache()
    //    val userData = Array("1887401,121.123,23.4214,2017-01-01 06:02:20",
    //      "1887401,121.123,23.4214,2017-01-01 06:02:20"
    //    )
    //    val userRDD = sc.parallelize(userData) //生成rdd
    //    val userRddROW = userRDD.map(row => {
    //        val split = row.split(",");
    //        Row(split(0), split(1).toFloat, split(2) toFloat, split(3))
    //      })
    var resultRdd = userRddROW.mapPartitions(itr => {
      val list = new ListBuffer[(String, Int)]()
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      while (itr.hasNext) {
        val r = itr.next()
        val sb = new StringBuilder()
        for (i <- 0 until quyus.size()) {
          var flag = "0"
          try {
            if (sdf.parse(r.getString(3)).getDate - sdf.parse(quyus.get(i).getDate).getDate < quyus.get(i).getInterval.toLong * 3600) {
              //是否在区域内
              if (existErea(quyus.get(i), r.getFloat(1), r.getFloat(2))) {
                flag = "1"
              }
            }
          }catch {
            case e:Exception=>
          }
          sb.append(flag)
        }
        list.append((r.getString(0), Integer.parseInt(sb.toString(), 2)))
      }
      list.iterator
    })
  println("***103")
  resultRdd.filter(v=>v._1.split("@")(0).equals("18273516886")).foreach(println(_))
  resultRdd=resultRdd.reduceByKey(_ & _).map(r => {
      var c = 0
      var n = r._2
      while (n != 0) {
        c += 1
        n = n & (n - 1)
      }
      (r._1, c)
    })
  println("***114")
  resultRdd.filter(v=>v._1.split("@")(0).equals("18273516886")).foreach(println(_))
  resultRdd=resultRdd.filter(r => r._2 >= relation) //满足关联度的数据过滤
  println("***111"+resultRdd.count())
    resultRdd.foreachPartition(itr => {
      val conn = DBFactory.getInstance.getConnection;
      var count = 1
      val sql="INSERT INTO \"WJ_BIGDATA\".\"SNP_RESULT\" (\"RELATION\", \"TASKID\", \"MOBILE\", \"IDCARD\", \"USERNAME\",\"ID\") VALUES(?,?,?,?,?,?)"
      val ps=conn.prepareStatement(sql)
      while (itr.hasNext) {
        val row = itr.next()
        ps.setInt(1, row._2)
        ps.setString(2, taskobj.getTaskid)
        ps.setString(3, row._1.split("@")(0))
        ps.setString(4, row._1.split("@")(2))
        ps.setString(5, row._1.split("@")(1))
        ps.setString(6,UUID.randomUUID().toString)
        ps.addBatch()
        count+=1
        if (count % 1000 == 0) {
          ps.executeBatch()
          conn.commit()
        }
      }
      ps.executeBatch()
      conn.commit()
      ps.close()
      conn.close()
    })

  }

  /**
    * 是否在范围内
    *
    * @param quyu
    * @return
    */
  def existErea(quyu: QuyuModel, lat: Float, lng: Float): Boolean = {
    if(quyu.getType.equals("Circle")){
      val point: String = quyu.getPoint
      val r: Float = quyu.getRadius.toFloat
      val index: Int = point.indexOf(',')
      val x1: Float = lat
      val y1: Float = lng
      val r1: Float = (Math.sqrt(Math.pow(x1 - point.substring(1, index).toFloat, 2) + Math.pow(y1 - point.substring(index + 1, point.length - 1).toFloat, 2))).toFloat
      if (r1 <= r) {
        return true
      }
      else {
        return false
      }
    } else {
      val low_left: String = quyu.getLower_left
      val up_right: String = quyu.getUpper_right
      val index1: Int = low_left.indexOf(',')
      val xmin: Float = low_left.substring(1, index1).toFloat
      val ymin: Float = low_left.substring(index1 + 1, low_left.length - 1).toFloat

      val index2: Int = up_right.indexOf(',')
      val xmax: Float = up_right.substring(1, index2).toFloat
      val ymax: Float = up_right.substring(index2 + 1, up_right.length - 1).toFloat

      val r: KeyAreaModel.Rectangle = new KeyAreaModel.Rectangle
      r.setXmin(xmin)
      r.setXmax(xmax)
      r.setYmin(ymin)
      r.setYmax(ymax)
      if (lng >= r.getXmin() && lng <= r.getXmax() && lat >= r.getYmin() && lat <= r.getYmax())
        return true;
      else
        return false;
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
