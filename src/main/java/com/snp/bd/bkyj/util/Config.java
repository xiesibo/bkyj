package com.snp.bd.bkyj.util;

/**
 * Created by x on 2018/8/3.
 */
public class Config {
    public static String SUBMIT_HOST="192.168.1.41";
    public static String SUBMIT_HOST_USER="root";
    public static String SUBMIT_PASSWORD="dsp@2529o7855";
    public static String SPARK_SUBMIT="kinit -kt /user.keytab nifi && spark-submit --conf spark.yarn.keytab=/user.keytab --conf spark.yarn.principal=nifi@HADOOP.COM";
}
