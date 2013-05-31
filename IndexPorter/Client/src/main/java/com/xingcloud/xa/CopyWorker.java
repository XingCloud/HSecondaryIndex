package com.xingcloud.xa;

import com.xingcloud.xa.coprocessor.IndexCopyProtocol;
import com.xingcloud.xa.util.BytesUtil;
import com.xingcloud.xa.util.DateUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 5/27/13
 * Time: 11:39 AM
 * To change this template use File | Settings | File Templates.
 */
public class CopyWorker implements Runnable {
    private HBaseAdmin admin;
    private Configuration config;
    private String tableName;
    private Map<String, Integer> properties;
    private ExecutorService executor;
    private AtomicInteger totalCount;
    private byte[] today;
    private byte[] yesterday;

    private static Log LOG = LogFactory.getLog(CopyWorker.class);

    public CopyWorker(HBaseAdmin admin, String tableName, Map<String, Integer> properties, Configuration config) {
        this.admin = admin;
        this.tableName = tableName;
        this.properties = properties;
        this.config = config;
        this.executor = Executors.newFixedThreadPool(4);
        this.totalCount = new AtomicInteger(0);
        today = Bytes.toBytes(DateUtil.getTodayDateStr());
        yesterday = Bytes.toBytes(DateUtil.getYesterdayDateStr());
    }

    @Override
    public void run() {

        LOG.info("Copy start , index table : " + tableName);
        long start = System.currentTimeMillis();
        for (String property : properties.keySet()) {
            executor.execute(new CoprocessorWorker(tableName, property));
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
        }
        long timeCost = System.currentTimeMillis() - start;
        LOG.info("Copy finished ,table : `" + tableName + "`, totalCount " + totalCount + ",cost " + timeCost / 1000 + "seconds .");
    }

    class CoprocessorWorker implements Runnable {
        private String tableName;
        private String property;

        CoprocessorWorker(String tableName, String property) {
            this.tableName = tableName;
            this.property = property;
        }

        @Override
        public void run() {
            try {
                LOG.info("Copy property `" + property + "` in table `" + tableName + "` begin .");
                short propertyID = properties.get(property).shortValue();
                final byte[] startRow = BytesUtil.CombineBytes(Bytes.toBytes(propertyID), yesterday);
                final byte[] stopRow = BytesUtil.CombineBytes(Bytes.toBytes(propertyID), today);
                HTable hTable = new HTable(config, tableName);
                Map<byte[], Integer> result = hTable.coprocessorExec(IndexCopyProtocol.class, startRow, stopRow, new Batch.Call<IndexCopyProtocol, Integer>() {
                    @Override
                    public Integer call(IndexCopyProtocol indexCopyProtocol) throws IOException {
                        return indexCopyProtocol.copyIndex(startRow, stopRow, tableName,property);
                    }
                });
                int count = 0 ;
                for(int value : result.values()){
                    count += value ;
                }
                totalCount.addAndGet(count);
                LOG.info("Copy property `" + property + "` in table `" + tableName + "` finish, count : " + count);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }
    }

}
