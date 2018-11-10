package com.snp.bd.bkyj.hdfs;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;
import java.io.File;
import java.io.IOException;

/**
 * Created by Administrator on 2017/12/10.
 */
public class Test {

    private static String PRNCIPAL_NAME = "nifi@HADOOP.COM";
    private static String PATH_TO_KEYTAB = Test.class.getClassLoader().getResource("user.keytab").getPath();
    private static String PATH_TO_KRB5_CONF = Test.class.getClassLoader().getResource("krb5.conf").getPath();
    private static String PATH_TO_HDFS_SITE_XML = Test.class.getClassLoader().getResource("hdfs-site.xml")
            .getPath();
    private static String PATH_TO_CORE_SITE_XML = Test.class.getClassLoader().getResource("core-site.xml")
            .getPath();


    public Test(){

        //confload
        Configuration conf = new Configuration();
        // conf file
        conf.addResource(new Path(PATH_TO_HDFS_SITE_XML));
        conf.addResource(new Path(PATH_TO_CORE_SITE_XML));


        //authorization
        if ("kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication"))) {
            System.setProperty("java.security.krb5.conf", PATH_TO_KRB5_CONF);
            try {
                LoginUtil.login(PRNCIPAL_NAME, PATH_TO_KEYTAB, PATH_TO_KRB5_CONF, conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("hdfs 登录成功！");



    }

    public static void main(String args[]){
//        String path = Test.class.getClassLoader().getResource("krb5.conf").getPath();
//        System.out.println(path);
//
//        String userdir = System.getProperty("user.dir") + File.separator + "krb" + File.separator;
//        System.out.println(userdir);
        new Test();
    }
}
