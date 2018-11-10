package com.snp.bd.bkyj.main

import java.io.File
import java.util
import com.snp.bd.bkyj.model.Msg
import kafka.api.OffsetRequest
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.dstream.InputDStream
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.fs.Path
import com.snp.bd.bkyj.dataflow.LoginUtil
import com.snp.bd.bkyj.kafka.KafkaSink
import com.snp.bd.bkyj.msg.{AbstractMessage, HDMessage}
import com.snp.bd.bkyj.rule.RuleCompareService
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wxh-pc on 2017/4/27.
  */
object Bkyj {
  val log = org.apache.log4j.LogManager.getLogger("SparkStreamingBkyj")
  val batchDuration=1000
  def main(args: Array[String]): Unit = {
//    val userKeytabPath: String = "krb/user.keytab"
//    val userKeyconfPath: String = "krb/krb5.conf"

//    val userKeytabPath: String = new File("user.keytab").getAbsolutePath
//    val userKeyconfPath: String =new File("krb5.conf").getAbsolutePath
    val userKeytabPath: String = "/user.keytab"
    val userKeyconfPath: String = "/krb5.conf"
    val userPrincipal: String = "nifi@HADOOP.COM"
    LoginUtil.login(userPrincipal, userKeytabPath, userKeyconfPath, new Configuration)
    log.info("创建StreamingContext")
    //创建StreamingContext
    val ssc=createStreamingContext()
    //开始执行
    ssc.start()
    stopByMarkFile(ssc)       //通过扫描HDFS文件来优雅的关闭
    //等待任务终止
    ssc.awaitTermination()
  }

  def createStreamingContext(): StreamingContext = {
    var args = Array("192.168.1.46:21005,192.168.1.47:21005,192.168.1.48:21005,192.168.1.49:21005,192.168.1.50:21005,192.168.1.51:21005", "bkyj_topic")
    //args[0]   brokers
    //args[1]    topics
    if (args.length != 2) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args
    val dest_topic = "bkyj_warn_topic"
    val sparkConf = new SparkConf().setAppName("bkyj").setMaster("local[5]")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true") //优雅的关闭
    sparkConf.set("spark.streaming.backpressure.enabled", "true") //激活削峰功能
    sparkConf.set("spark.streaming.backpressure.initialRate", "5000") //第一次读取的最大数据值
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "2000") //每个进程每秒最多从kafka读取的数据条数
    val ssc = new StreamingContext(sparkConf, Milliseconds(batchDuration))
    // var bv =  ssc.sparkContext.broadcast(p.init())
    //  p.monitor(bv,ssc.sparkContext)

    val topicSet = topics.split(",").toSet
    var kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      //        "key.deserializer" -> classOf[StringDeserializer],
      //        "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "unique_group_id"
    )
    val firstReadLastest=true//第一次启动是否从最新的开始消费
    if (firstReadLastest)kafkaParams += ("auto.offset.reset"-> OffsetRequest.LargestTimeString)//从最新的开始消费
    val conf = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.serializer" -> classOf[org.apache.kafka.common.serialization.StringSerializer].getName,
      // "key.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer].getName,
      "value.serializer" -> classOf[org.apache.kafka.common.serialization.StringSerializer].getName
      //  "value.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer].getName
    )
    val kafkaSink = ssc.sparkContext.broadcast(KafkaSink[String, String](conf))
    //创建zkClient注意最后一个参数最好是ZKStringSerializer类型的，不然写进去zk里面的偏移量是乱码
/*    val  zkClient= new ZkClient("192.168.1.46:24002,192.168.1.47:24002,192.168.1.48:24002", 30000, 30000,ZKStringSerializer)
    val zkOffsetPath="/bkyj/offset"//zk的路径*/
    //val lines = createKafkaStream(ssc,kafkaParams,zkClient,zkOffsetPath,topicSet).map(_._2)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet).map(_._2)
    //ssc.checkpoint("/spark/bkyj/checkpoint")   // 设置在HDFS上的checkpoint目录
    //设置通过间隔时间，定时持久checkpoint到hdfs上
    //lines.checkpoint(Seconds(batchDuration*5))
    lines.foreachRDD {
      x => {
        if (!x.isEmpty()) {
          x.foreach(y => {
            println("recive message:"+y)
            val am = Msg.convert(y);
            if (am != null) {
                //与规则进行比对
                val warns = RuleCompareService.compare(am)
                if (warns != null && warns.size() > 0) {
                  val it = warns.iterator()
                  while (it.hasNext) {
                    val w = it.next()
                    println("########################:" + w.toJSON)
                    kafkaSink.value.send(dest_topic, w.toJSON)
                  }
                }

            }

          })
        }
      }
        //更新每个批次的偏移量到zk中，注意这段代码是在driver上执行的
        //KafkaOffsetManager.saveOffsets(zkClient,zkOffsetPath,x)
    }
    ssc
  }

  def getImeis(): util.ArrayList[String] = {
    val l = new java.util.ArrayList[String]()
    l.add("86491303901950")
    return l
  }

  /** *
    * 通过一个消息文件来定时触发是否需要关闭流程序
    *
    * @param ssc StreamingContext
    */
  def stopByMarkFile(ssc: StreamingContext): Unit = {
    val intervalMills = 10 * 1000 // 每隔10秒扫描一次消息是否存在
    var isStop = false
    val hdfs_file_path = "/spark/streaming/stop" //判断消息文件是否存在，如果存在就
    while (!isStop) {
      isStop = ssc.awaitTerminationOrTimeout(intervalMills)
      if (!isStop && isExistsMarkFile(hdfs_file_path)) {
        log.warn("2秒后开始关闭sparstreaming程序.....")
        Thread.sleep(2000)
        ssc.stop(true, true)
      }

    }
  }

  /** *
    * 判断是否存在mark file
    *
    * @param hdfs_file_path mark文件的路径
    * @return
    */
  def isExistsMarkFile(hdfs_file_path: String): Boolean = {
    val conf = new Configuration()
    val path = new Path(hdfs_file_path)
    val fs = path.getFileSystem(conf);
    println("***hdfsfile:"+fs.exists(path))
    fs.exists(path)
  }
  /****
    *
    * @param ssc  StreamingContext
    * @param kafkaParams  配置kafka的参数
    * @param zkClient  zk连接的client
    * @param zkOffsetPath zk里面偏移量的路径
    * @param topics     需要处理的topic
    * @return   InputDStream[(String, String)] 返回输入流
    */
  def createKafkaStream(ssc: StreamingContext,
                        kafkaParams: Map[String, String],
                        zkClient: ZkClient,
                        zkOffsetPath: String,
                        topics: Set[String]): InputDStream[(String, String)]={
    //目前仅支持一个topic的偏移量处理，读取zk里面偏移量字符串
    val zkOffsetData=KafkaOffsetManager.readOffsets(zkClient,zkOffsetPath,topics.last)

    val kafkaStream = zkOffsetData match {
      case None =>  //如果从zk里面没有读到偏移量，就说明是系统第一次启动
        log.info("系统第一次启动，没有读取到偏移量，默认就最新的offset开始消费")
        //使用最新的偏移量创建DirectStream
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      case Some(lastStopOffset) =>
        log.info("从zk中读取到偏移量，从上次的偏移量开始消费数据......")
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        //使用上次停止时候的偏移量创建DirectStream
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, lastStopOffset, messageHandler)
    }
    kafkaStream//返回创建的kafkaStream
  }
}
