package com.snp.bd.bkyj.redis;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.huawei.medis.ClusterBatch;
import com.snp.bd.bkyj.model.StateModel;
import org.apache.log4j.Logger;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Administrator on 2017/12/2.
 */
public class RedisService implements Serializable{

    private JedisCluster client;
    private ClusterBatch pipeline;
    private Logger logger = Logger.getLogger("Redis服务");
    public static RedisService getInstance() {
        return RedisService.DataServiceHolder.instance;
    }

    private static class DataServiceHolder {
        private static RedisService instance = new RedisService();
    }

    private RedisService() {
        Set<HostAndPort> hosts = new HashSet<HostAndPort>();
        hosts.add(new HostAndPort("192.168.1.49",22400));
        hosts.add(new HostAndPort("192.168.1.50",22400));
        hosts.add(new HostAndPort("192.168.1.51",22400));
        //hosts.add(new HostAndPort("192.168.0.2",22401));
        int timeout = 5000;
        client = new JedisCluster(hosts, timeout);
        logger.info("Redis连接成功!");
    }

    public StateModel getState(String key){
        String json = get(key);
        if(json == null) return null;
        try {
            StateModel sm = JSONObject.parseObject(json, StateModel.class);
            return sm;
        }catch (Exception e){
            logger.warn("解析json异常:"+json);
            return null;
        }
    }


    public void set(StateModel state,String... keys){
        if(keys != null)
            for(String key:keys)set(key, JSONObject.toJSONString(state));
    }

    public void set(String key,String value){
        client.set(key,value);
    }

    public String get(String key){
        return client.get(key);
    }

    public static void main(String args[]){
        //RedisService.getInstance().set("15802540365","wxh");

        System.out.println(RedisService.getInstance().get("1948017989"));
    }

}
