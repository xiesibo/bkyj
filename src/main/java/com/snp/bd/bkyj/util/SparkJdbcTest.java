package com.snp.bd.bkyj.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.hive.beeline.BeeLine;
import com.snp.bd.bkyj.dataflow.LoginUtil;

/**
 * Created by x on 2018/6/9.
 */
public class SparkJdbcTest {
    public static void main(String[] args) {
        System.out.println(SparkJdbcTest.class.getResource("/").toString());

        String userdir = System.getProperty("user.dir") + "\\";
        String userPrincipal = "nifi@HADOOP.COM";
        String userKeytabPath = userdir + "krb\\user.keytab";
        String userKeyconfPath = userdir + "krb\\krb5.conf";
        String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
        String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
        String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";
        String userName = "nifi";
        Configuration hconf = HBaseConfiguration.create();
//        hconf.addResource(new Path("core-site.xml"));
//        hconf.addResource(new Path("hdfs-site.xml"));
//        hconf.addResource(new Path("hbase-site.xml"));
//        hconf.addResource(new Path("hive-site.xml"));
        System.out.println(hconf.get("hadoop.registry.zk.root"));
        try {
            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabPath);
            LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
            LoginUtil.login(userName, userKeytabPath, userKeyconfPath, hconf);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String securityConfig = "saslQop=auth-conf;auth=KERBEROS;principal=spark/hadoop.hadoop.com@HADOOP.COM" + ";";

        String zk="zk.quorum=192.168.1.47,192.168.1.46,192.168.1.48;zk.port=24002;spark.thriftserver.zookeeper.dir=/thriftserver;";
        String HA_CLUSTER_URL = "ha-cluster";
        StringBuilder sb = new StringBuilder("jdbc:hive2://" + HA_CLUSTER_URL + "/default;"+zk + securityConfig);
        String url = sb.toString();
        ArrayList<String> sqlList = new ArrayList<String>();
        sqlList.add("show tables");
        try {
            executeSql(url,sqlList);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    static void executeSql(String url, ArrayList<String> sqls) throws ClassNotFoundException, SQLException {

        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            ;
        }
        Connection connection = null;
        PreparedStatement statement = null;

        try {
            connection = DriverManager.getConnection(url);
            for (int i = 0; i < sqls.size(); i++) {
                String sql = sqls.get(i);
                System.out.println("---- Begin executing sql: " + sql + " ----");
                statement = connection.prepareStatement(sql);
                ResultSet result = statement.executeQuery();
                ResultSetMetaData resultMetaData = result.getMetaData();
                Integer colNum = resultMetaData.getColumnCount();
                for (int j = 1; j < colNum; j++) {
                    System.out.println(resultMetaData.getColumnLabel(j) + "\t");
                }
                System.out.println();

                while (result.next()) {
                    for (int j = 1; j < colNum; j++) {
                        System.out.println(result.getString(j) + "\t");
                    }
                    System.out.println();
                }
                System.out.println("---- Done executing sql: " + sql + " ----");
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != statement) {
                statement.close();
            }
            if (null != connection) {
                connection.close();
            }
        }
    }
}
