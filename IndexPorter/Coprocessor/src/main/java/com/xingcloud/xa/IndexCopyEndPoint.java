package com.xingcloud.xa;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import com.xingcloud.xa.coprocessor.IndexCopyProtocol;
import com.xingcloud.xa.util.BytesUtil;
import com.xingcloud.xa.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 5/28/13
 * Time: 2:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class IndexCopyEndPoint extends BaseEndpointCoprocessor implements IndexCopyProtocol {

    private final static int batchSize = 4 * 1024;
    private final static short writeThreadsSize = 2;

    private Configuration config;
    private String tableName;
    private String property;
    private AtomicInteger totalCount = new AtomicInteger(0);
    private AtomicInteger finishedCount = new AtomicInteger(0);
    private String regionName;

    private static Log LOG = LogFactory.getLog(IndexCopyEndPoint.class);

    @Override
    public int copyIndex(byte[] startKey, byte[] stopKey, String tableName, String property) {
        RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();
        regionName = Bytes.toString(environment.getRegion().getRegionName());
        config = HBaseConfiguration.create();
        this.tableName = tableName;
        this.property = property;
        byte[] today = Bytes.toBytes(DateUtil.getTodayDateStr());
        List<KeyValue> results = new ArrayList<KeyValue>();
        List<Put> dataPuts = new ArrayList<Put>(batchSize);
        ExecutorService executor = Executors.newFixedThreadPool(writeThreadsSize);

        Scan scan = new Scan();
        scan.setStartRow(startKey);
        scan.setStopRow(stopKey);

        try {
            InternalScanner scanner = environment.getRegion().getScanner(scan);
            int kvNums = 0 ;
            while (scanner.next(results)) {
                KeyValue kv = results.get(0);
                byte[] rowKey = kv.getRow();
                BytesUtil.replaceBytes(today, 0, rowKey, 2, today.length);
                Put put = new Put(rowKey);
                for ( KeyValue keyValue : results) {
                    put.add(keyValue.getFamily(), keyValue.getQualifier(), keyValue.getValue());
                    kvNums ++ ;
                }
                dataPuts.add(put);

                if (dataPuts.size() >= batchSize || kvNums >= batchSize) {
                    kvNums = 0 ;
                    executor.execute(new Writer(dataPuts));
                    dataPuts = new ArrayList<Put>(batchSize);
                    totalCount.addAndGet(dataPuts.size());
                }
                results.clear();
            }
            if (dataPuts.size() != 0) {
                executor.execute(new Writer(dataPuts));
                totalCount.addAndGet(dataPuts.size());
            }
            scanner.close();
            executor.shutdown();
            while (!executor.isTerminated()) {
                Thread.sleep(100);
            }

        } catch (IOException e) {
            LOG.error(e.getMessage());

        } catch (InterruptedException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
        return finishedCount.get();
    }

    class Writer implements Runnable {
        private List<Put> dataPuts;

        Writer(List<Put> dataPuts) {
            this.dataPuts = dataPuts;
        }

        @Override
        public void run() {
            try {
                HTable htable = new HTable(config, tableName);
                htable.put(dataPuts);
                htable.close();
                int countTotal = totalCount.get();
                int countFinished = finishedCount.addAndGet(dataPuts.size());
                LOG.info(countFinished + "/" + countTotal + " ,`" + regionName + "`.`" + tableName + "`.`" + property + "`");

            } catch (IOException e) {
                LOG.error(e.getMessage());
            }


        }
    }
}
