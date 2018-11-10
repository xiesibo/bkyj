package com.snp.bd.bkyj.rule;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.*;

/**
 *
 */
public class DBFactory {


    public static final String url = "jdbc:oracle:thin:@192.168.1.8:1521:orcl";
    public static final String driver = "oracle.jdbc.driver.OracleDriver";
    public static final String username = "wj_bigdata";
    public static final String password = "amSlbtTaj";
    BasicDataSource dataSource;
    private static DBFactory db = null;

    private DBFactory() {
        dataSource = new BasicDataSource();
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setUrl(url);
        dataSource.setDriverClassName(driver);
        dataSource.setInitialSize(50);
        dataSource.setMinIdle(10);
    }

    public static DBFactory getInstance() {
        if (db == null) {
            db = new DBFactory();
        }
        return db;
    }
    public  Connection getConnection() throws SQLException{
        return dataSource.getConnection();
    }

    public static  void close(ResultSet rs, PreparedStatement ps, Connection conn) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
