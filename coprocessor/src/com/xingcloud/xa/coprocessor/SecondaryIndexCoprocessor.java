package com.xingcloud.xa.coprocessor;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * User: Jian Fang
 * Date: 13-5-13
 * Time: 下午2:28
 */
public class SecondaryIndexCoprocessor extends BaseRegionObserver {
    private HTablePool pool;

    private static Log LOG = LogFactory.getLog(SecondaryIndexCoprocessor.class);
    private static JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost");
    private static BlockingQueue<String> redisJobs = new LinkedBlockingQueue<String>();
    static{
        new Thread(new RedisJobConsumer(redisJobs, jedisPool)).start();
    }
    private Cache blockingCache;

    @Override
    public void start(CoprocessorEnvironment e){
        blockingCache = CacheManager.getInstance().getCache("blocking_cache");

    }

    @Override
    public void prePut(
            final ObserverContext<RegionCoprocessorEnvironment> observerContext,
            final Put put,
            final WALEdit edit,
            final boolean writeToWAL)
            throws IOException {
        byte[] table  = observerContext.getEnvironment().getRegion().getRegionInfo().getTableName();
        String tableName = Bytes.toString(table);

        if(!tableName.startsWith("property_") || tableName.endsWith("_index")){
            return;
        }
        int index = tableName.lastIndexOf("_");
        String projectID = tableName.substring(0, index);
        int propertyID = Integer.parseInt(tableName.substring(index + 1));

        long s1 = System.nanoTime();
        HTableInterface dataTable = observerContext.getEnvironment().getTable(table);
        byte[] newValue = put.get(Bytes.toBytes("value"), Bytes.toBytes("value")).get(0).getValue();
        byte[] oldValue = getValue(dataTable, tableName, put.getRow());
        if(oldValue == null){
            submitIndexJob(projectID, false, put.getRow(), propertyID, null, newValue);
        } else if(!Bytes.equals(oldValue, newValue)){
            submitIndexJob(projectID, true, put.getRow(), propertyID, oldValue, newValue);
        }
        /*
        HTableInterface indexTable = observerContext.getEnvironment().getTable(Bytes.toBytes(projectID + "_index"));
        if(oldValue != null){
            if(!Bytes.equals(oldValue, newValue)){
                byte[] indexRowKey = combineIndexRowKey(propertyID, oldValue);
                Delete delete = new Delete(indexRowKey);
                delete.deleteColumn(Bytes.toBytes("value"), put.getRow(), 0);
                indexTable.delete(delete);
            }
        }

        if(oldValue == null || !Bytes.equals(oldValue, newValue)){
            byte[] indexRowKey = combineIndexRowKey(propertyID, newValue);
            Put indexPut = new Put(indexRowKey, 0);
            indexPut.setWriteToWAL(false);
            indexPut.add(Bytes.toBytes("value"), put.getRow(), Bytes.toBytes(true));
            indexTable.put(indexPut);
        }
        indexTable.close();
        */

        dataTable.close();
        if(oldValue == null || !Bytes.equals(oldValue, newValue)){
            blockingCache.put(new Element(tableName + Bytes.toString(put.getRow()), Bytes.toString(newValue)));
        }

    }

    private byte[] combineIndexRowKey(int propertyID, byte[] value){
        return bytesCombine(Bytes.toBytes((short)propertyID), value);

    }
    private byte[] bytesCombine(byte[]... bytesArrays){
        int length = 0;
        for (byte[] bytes: bytesArrays){
            length += bytes.length;
        }
        byte[] combinedBytes = new byte[length];
        int index = 0;
        for (byte[] bytes: bytesArrays){
            for(byte b: bytes){
                combinedBytes[index] = b;
                index++;
            }
        }
        return combinedBytes;
    }

    private byte[] getValue(HTableInterface table, String tableName, byte[] uid) throws IOException {
        String key = tableName + Bytes.toString(uid);
        Element element = blockingCache.get(key);
        if (element != null) {
            return Bytes.toBytes((String) element.getObjectValue());
        } else {
            Get get = new Get(uid);
            Result result = table.get(get);
            if(result.isEmpty()){
                return null;
            } else {
                String value = Bytes.toString(result.getValue(Bytes.toBytes("value"), Bytes.toBytes("value")));
                blockingCache.put(new Element(key, value));
                return Bytes.toBytes(value);
            }
        }
    }

    private void submitIndexJob(String projectID, boolean shouldDel, byte[] uid,
                                int propertyID, byte[] oldValue, byte[] newValue){
        byte[] convertedUid = {0,0,0,0,0,0,0,0};
        for(int i = 0; i < 5; i++)
            convertedUid[i+3] = uid[i];
        String job;
        job = "{\"uid\": " + Bytes.toLong(convertedUid) +
                ", \"propertyID\": " + propertyID +
                ", \"old_value\": \"" + Bytes.toString(oldValue) +
                "\", \"new_value\": \"" + Bytes.toString(newValue) +
                "\", \"delete\": " + shouldDel + ", \"pid\": \"" + projectID + "\"}";
        redisJobs.add(job);
    }

}
