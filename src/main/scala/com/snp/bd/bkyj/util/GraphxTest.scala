package com.snp.bd.bkyj.util

import java.io.File

import com.snp.bd.bkyj.dataflow.LoginUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.{Graph, Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/6/22 0022.
  */
object GraphxTest {
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
  val sparkConf = new SparkConf().setAppName("Pzfx").set("spark.inputFormat.cache.enabled", "false").set("spark.hbase.obtainToken.enabled", "true").set("spark.sql.authorization.enabled", "true").set("hive.security.authorization.enabled", "true")
  sparkConf.setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  def main(args: Array[String]) {

/*    // Create an RDD for the vertices
    val users: RDD[(VertexId,String)] =
      sc.parallelize(Array((3L,"a"), (7L,"a"),
        (5L,"a"), (2L, "a")))
    // Create an RDD for edges
    val relationships: RDD[(VertexId, VertexId)] =
      sc.parallelize(Array((3L, 7L),    (5L, 3L),
        (2L, 6L),(6L, 8L),(2L, 5L), (2L, 5L), (5L, 7L)))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe")
    // Build the initial Graph
    //val graph = Graph(users, relationships, defaultUser)
    GraphNdegUtil.aggNdegreedVertices(relationships,sc.parallelize(Array(2L)),3).foreach(println(_))*/
val userInfo = Array("1,2",
  "5,2"
)
    val vertices = sc.parallelize(userInfo).map(r => {
      val fields = r.split(",")
      (fields(0).toLong, fields(1))
    })
    val relation = Array("1,2",
      "2,3",
      "1,4",
      "1,5",
      "4,3",
      "5,4"
    )
    val relationrdd = sc.parallelize(relation)
    val edges = relationrdd.map(r => {
      val split = r.split(",")
      Edge(split(0).toLong, split(1).toLong, 0)
    })
    val graph = Graph(vertices, edges, "").persist()
    // 要求最短路径的点集合
    val landmarks = Seq(1, 3).map(_.toLong)

    // 计算最短路径
    val results = ShortestPaths.run(graph, landmarks).vertices.collect.map {
      case (v, spMap) => (v, spMap.mapValues(i => i))
    }

    val shortestPath1 = ShortestPaths.run(graph, landmarks)
    // 与真实结果对比
    println("\ngraph edges");
    println("edges:");
    graph.edges.collect.foreach(println)
    //    graph.edges.collect.foreach(println)
    println("vertices:");
    graph.vertices.collect.foreach(println)
    //    println("triplets:");
    //    graph.triplets.collect.foreach(println)
    println();

    println("\n shortestPath1");
    println("edges:");
    shortestPath1.edges.collect.foreach(println)
    println("vertices:");
    shortestPath1.vertices.collect.foreach(println)
    //    println("vertices:")


    println("results.toSet:" + results.toSet);
    println("end");

  }
}
