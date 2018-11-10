package com.snp.bd.bkyj.main;

import com.alibaba.fastjson.JSONObject;
import com.snp.bd.bkyj.model.GxfxModel;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by x on 2018/5/24.
 */
public class Ts {
    public static void main(String[] args) {
            String[] arr="v5,v6,v7,v8,v11,v12,v13,v14,v15,v18".split(",");
            String temp="(case max(v5)-min(v5) when 0 then 1 else max(v5)-min(v5) end) mmv5,";

        for (String s:arr
             ) {
            System.out.print(temp.replace("v5",s));
        }

    }
    private static void t(Map<String,String> map){
        map.put("q","1");
    }
    public static void execDML(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        ResultSetMetaData resultMetaData = null;

        try {
            // 执行HQL
            statement = connection.prepareStatement(sql);
            resultSet = statement.executeQuery();

            // 输出查询的列名到控制台
            resultMetaData = resultSet.getMetaData();
            int columnCount = resultMetaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(resultMetaData.getColumnLabel(i) + '\t');
            }
            System.out.println();

            // 输出查询结果到控制台
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(resultSet.getString(i) + '\t');
                }
                System.out.println();
            }
        }
        finally {
            if (null != resultSet) {
                resultSet.close();
            }

            if (null != statement) {
                statement.close();
            }
        }
    }
}
