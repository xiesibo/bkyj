package com.snp.bd.bkyj.hbase;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

//import static oracle.net.aso.C03.e;

/**
 * Created by Administrator on 2017/12/11.
 */
public class HBaseClient implements Serializable{
    /**
     * 写hbase
     * @param table
     * @param rowkey
     * @param family
     * @param column
     * @param value
     */
    public static void put(String table, String rowkey,String family, String column, String value)throws Exception{
        Map<String,String> body = new HashMap<String,String>();
        body.put("table",table);
        body.put("rowkey",rowkey);
        body.put("family",family);
        body.put("column",column);
        body.put("value",value);
//        try {
//           Response response =  Request.Put("http://192.168.1.139:8080/api/v1/hbases/records").bodyString(JSONObject.toJSONString(body), ContentType.APPLICATION_JSON).execute();
//           System.out.println(response.returnResponse().getStatusLine());
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw e;
//        }

        URL url = new URL("http://192.168.1.139:8080/api/v1/hbases/records");
        try {
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setRequestMethod("PUT");
            conn.setRequestProperty("Content-Type", " application/json");
            conn.setDoInput(true);
            conn.setDoOutput(true);
            OutputStream os = conn.getOutputStream();
            os.write(JSONObject.toJSONString(body).getBytes("utf-8"));
            os.flush();
            os.close();

            // 定义 BufferedReader输入流来读取URL的响应
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream(),"utf-8"));
            StringBuilder sb = new StringBuilder();
            String line="";
            while ((line = reader.readLine()) != null){
                sb.append(line);
            }
            reader.close();
            conn.disconnect();
            System.out.println(sb.toString());
//            BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
//            String line = "";
//            String result = "";
//            while( (line =br.readLine()) != null ){
//                result += line;
//            }
//            log.info(result);
//            br.close();
        }catch(Exception e){
                e.printStackTrace();
        }
    }

    public static void main(String args[]){
        try {
            put("rwhx_table","15802540365","f","a","3");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
