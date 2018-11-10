package com.snp.bd.bkyj.dataflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * Created by wxh-pc on 2017/9/13.
 */
public class KafkaConsumerClient {

    public static void main(String args[])throws IOException{

/*        String userPrincipal = "nifi@HADOOP.COM";
        String userKeytabPath = "krb/user.keytab";
        String userKeyconfPath = "krb/krb5.conf";

        LoginUtil.login(userPrincipal,userKeytabPath,userKeyconfPath,new Configuration());
        Properties props = new Properties();

        props.put("bootstrap.servers", "192.168.1.38:21005,192.168.1.39:21005,192.168.1.40:21005");
        props.put("group.id", "wxh");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList("spark_stream"), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            }
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                //consumer.seekToEnd(new ArrayList<TopicPartition>());
            }
        });

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for(ConsumerRecord<String, String> record : records) {
                System.out.println("fetched from partition " + record.partition() + ", offset: " + record.offset() + ", message: " + record.value());
            }
            //按分区读取数据
//              for (TopicPartition partition : records.partitions()) {
//                  List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
//                  for (ConsumerRecord<String, String> record : partitionRecords) {
//                      System.out.println(record.offset() + ": " + record.value());
//                  }
//              }

        }*/
    }


}
