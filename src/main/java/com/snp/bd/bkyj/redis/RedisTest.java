package com.snp.bd.bkyj.redis;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.huawei.medis.BatchException;
import com.huawei.medis.ClusterBatch;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

public class RedisTest {
    private static final Logger LOGGER = Logger.getLogger(RedisTest.class);
    private JedisCluster client;
    private JedisPool pool;
    private ClusterBatch pipeline;

    public RedisTest() {
        Set<HostAndPort> hosts = new HashSet<HostAndPort>();
        hosts.add(new HostAndPort("192.168.1.38",22400));
        hosts.add(new HostAndPort("192.168.1.39",22402));
        hosts.add(new HostAndPort("192.168.1.40",22404));
//        hosts.add(new HostAndPort("192.168.1.40",22414));
        // add more host...

        // socket timeout(connect, read), unit: ms
        int timeout = 5000;
//        pool = new JedisPool("",100);
        client = new JedisCluster(hosts, timeout);
    }

    public void destory() {
        if (pipeline != null) {
            pipeline.close();
        }

        if (client != null) {
            client.close();
        }
    }

    public void testString() {
        String key = "sid-user01";

        // Save user's session ID, and set expire time
        client.setex(key, 5, "A0BC9869FBC92933255A37A1D21167B2");
        String sessionId = client.get(key);
        LOGGER.info("User " + key + ", session id: " + sessionId);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            LOGGER.warn("InterruptedException");
        }

        sessionId = client.get(key);
        LOGGER.info("User " + key + ", session id: " + sessionId);

        key = "message";

        client.set(key, "hello");
        String value = client.get(key);
        LOGGER.info("Value: " + value);

        client.append(key, " world");
        value = client.get(key);
        LOGGER.info("After append, value: " + value);

        client.del(key);
    }

    public void testList() {
        String key = "messages";

        // Right push
        client.rpush(key, "Hello how are you?");
        client.rpush(key, "Fine thanks. I'm having fun with redis.");
        client.rpush(key, "I should look into this NOSQL thing ASAP");

        // Fetch all data
        List<String> messages = client.lrange(key, 0, -1);
        LOGGER.info("All messages: " + messages);

        long len = client.llen(key);
        LOGGER.info("Message count: " + len);

        // Fetch the first element and delete it from list
        String message = client.lpop(key);
        LOGGER.info("First message: " + message);
        len = client.llen(key);
        LOGGER.info("After one pop, message count: " + len);

        client.del(key);
    }

    public void testHash() {
        String key = "userinfo-001";

        // like Map.put()
        client.hset(key, "id", "J001");
        client.hset(key, "name", "John");
        client.hset(key, "gender", "male");
        client.hset(key, "age", "35");
        client.hset(key, "salary", "1000000");

        // like Map.get()
        String id = client.hget(key, "id");
        String name = client.hget(key, "name");
        LOGGER.info("User " + id + "'s name is " + name);

        Map<String, String> user = client.hgetAll(key);
        LOGGER.info(user);
        client.del(key);

        key = "userinfo-002";
        Map<String, String> user2 = new HashMap<String, String>();
        user2.put("id", "L002");
        user2.put("name", "Lucy");
        user2.put("gender", "female");
        user2.put("age", "25");
        user2.put("salary", "200000");
        client.hmset(key, user2);
        client.hincrBy(key, "salary", 50000);
        id = client.hget(key, "id");
        String salary = client.hget(key, "salary");
        LOGGER.info("User " + id + "'s salary is " + salary);

        // like Map.keySet()
        Set<String> keys = client.hkeys(key);
        LOGGER.info("all fields: " + keys);
        // like Map.values()
        List<String> values = client.hvals(key);
        LOGGER.info("all values: " + values);

        // Fetch some fields
        values = client.hmget(key, "id", "name");
        LOGGER.info("partial field values: " + values);

        // like Map.containsKey();
        boolean exist = client.hexists(key, "gender");
        LOGGER.info("Exist field gender? " + exist);

        // like Map.remove();
        client.hdel(key, "age");
        keys = client.hkeys(key);
        LOGGER.info("after del field age, rest fields: " + keys);

        client.del(key);
    }

    public void testSet() {
        String key = "sets";

        client.sadd(key, "HashSet");
        client.sadd(key, "SortedSet");
        client.sadd(key, "TreeSet");

        // like Set.size()
        long size = client.scard(key);
        LOGGER.info("Set size: " + size);

        client.sadd(key, "SortedSet");
        size = client.scard(key);
        LOGGER.info("Set size: " + size);

        Set<String> sets = client.smembers(key);
        LOGGER.info("Set: " + sets);

        client.srem(key, "SortedSet");
        sets = client.smembers(key);
        LOGGER.info("Set: " + sets);

        boolean ismember = client.sismember(key, "TreeSet");
        LOGGER.info("TreeSet is set's member: " + ismember);

        client.del(key);
    }

    public void testSortedSet() {
        String key = "hackers";

        // Score: age
        client.zadd(key, 1940, "Alan Kay");
        client.zadd(key, 1953, "Richard Stallman");
        client.zadd(key, 1965, "Yukihiro Matsumoto");
        client.zadd(key, 1916, "Claude Shannon");
        client.zadd(key, 1969, "Linus Torvalds");
        client.zadd(key, 1912, "Alan Turing");

        // sort by score, ascending order
        Set<String> setValues = client.zrange(key, 0, -1);
        LOGGER.info("All hackers: " + setValues);

        long size = client.zcard(key);
        LOGGER.info("Size: " + size);

        Double score = client.zscore(key, "Linus Torvalds");
        LOGGER.info("Score: " + score);

        long count = client.zcount(key, 1960, 1969);
        LOGGER.info("Count: " + count);

        // sort by score, descending order
        Set<String> setValues2 = client.zrevrange(key, 0, -1);
        LOGGER.info("All hackers 2: " + setValues2);

        client.zrem(key, "Linus Torvalds");
        setValues = client.zrange(key, 0, -1);
        LOGGER.info("All hackers: " + setValues);

        client.del(key);
    }

    public void testKey() {
        String key = "test-key";

        client.set(key, "test");
        client.expire(key, 5);
        long ttl = client.ttl(key);
        LOGGER.info("TTL: " + ttl);

        String type = client.type(key);
        // KEY type may be string, list, hash, set, zset
        LOGGER.info("KEY type: " + type);

        client.del(key);
        client.rpush(key, "1");
        client.rpush(key, "4");
        client.rpush(key, "6");
        client.rpush(key, "3");
        client.rpush(key, "8");
        List<String> result = client.lrange(key, 0, -1);
        LOGGER.info("List: " + result);

        result = client.sort(key);
        LOGGER.info("Sort list: " + result);

        client.del(key);
    }

    public void testPipeline() {
        // Lazy load
        if (pipeline == null) {
            pipeline = client.getPipeline();
        }

        try {
            pipeline.hset("website", "google", "www.google.cn");
            pipeline.hset("website", "baidu", "www.baidu.com");
            pipeline.hset("website", "sina", "www.sina.com");

            Map<String, String> map = new HashMap<String, String>();
            map.put("cardid", "123456");
            map.put("username", "jzkangta");
            pipeline.hmset("hash", map);

            // submit
            pipeline.sync();

            pipeline.hget("website", "google");
            pipeline.hget("website", "baidu");
            pipeline.hget("website", "sina");

            // submit and get all return result
            List<Object> result = pipeline.syncAndReturnAll();
            LOGGER.info("Result: " + result);

            client.del("website");
            client.del("hash");
        } catch (BatchException e) {
            LOGGER.error("BatchException", e);
        }
    }

    private static String getResource(String name) {
        ClassLoader cl = RedisTest.class.getClassLoader();
        if (cl == null) {
            return null;
        }
        URL url = cl.getResource(name);
        if (url == null) {
            return null;
        }

        try {
            return URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

//    public static void init() throws IOException {
//        System.setProperty("redis.authentication.jaas", "false");
//
//        if (System.getProperty("redis.authentication.jaas", "false").equals("true")) {
//            String principal = "redisuser@HADOOP.COM";
//            LoginUtil.setJaasFile(principal, getResource("config/user.keytab"));
//            LoginUtil.setKrb5Config(getResource("config/krb5.conf"));
//        }
//    }

    public static void main(String[] args) {
//        try {
//            init();
//        } catch (IOException e) {
//            LOGGER.error("Failed to init security configuration", e);
//            return;
//        }


        System.out.println(RedisService.getInstance().get("13633665521@@2018-6-26@@34"));
//        test.testList();
//        test.testHash();
//        test.testSet();
//        test.testSortedSet();
//        test.testKey();
//        test.testPipeline();
//
//        test.destory();
    }
}
