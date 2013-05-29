package com.xingcloud.xa.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * User: Jian Fang
 * Date: 13-5-16
 * Time: 下午2:26
 */
public class RedisJobConsumer implements Runnable {

    private static Log LOG = LogFactory.getLog(RedisJobConsumer.class);

    private BlockingQueue<String> jobs;
    private List<String> batchJobs;
    private JedisPool jedisPool;

    public RedisJobConsumer(BlockingQueue<String> jobs, JedisPool jedisPool){
        this.jobs = jobs;
        this.batchJobs = new ArrayList<String>();
        this.jedisPool = jedisPool;
    }

    @Override
    public void run() {
        long sessionDuration = 0;
        while(true){
            try {
                String job = jobs.poll(5, TimeUnit.SECONDS);
                if(job != null) batchJobs.add(job);
                if(job == null || batchJobs.size() == 1000){
                    long start = System.nanoTime();
                    submitJobs();
                    long end = System.nanoTime();
                    sessionDuration+= (end - start);
                    if(job == null && batchJobs.size() > 0){
                        LOG.info("redis job session takes: " + sessionDuration + "ns");
                        sessionDuration = 0;
                    }
                    batchJobs.clear();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void submitJobs(){
        if(batchJobs.size() > 0){
            Jedis jedis = jedisPool.getResource();
            try{
                jedis.lpush("index_jobs", batchJobs.toArray(new String[batchJobs.size()]));
            } catch (Exception e){
                jedisPool.returnBrokenResource(jedis);
                jedis = null;
            } finally {
                if(jedis != null) jedisPool.returnResource(jedis);
            }
        }
    }
}
