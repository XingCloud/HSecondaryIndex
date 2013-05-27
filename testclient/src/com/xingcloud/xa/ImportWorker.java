package com.xingcloud.xa;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

import java.util.List;

/**
 * User: Jian Fang
 * Date: 13-5-9
 * Time: 下午5:56
 */
public class ImportWorker implements Runnable {
    private ImportWorkerInfo workerInfo;
    private ImportJob job;
    private HBaseAdmin admin;

    private static Log LOG = LogFactory.getLog(ImportWorker.class);

    public ImportWorker(ImportWorkerInfo workerInfo, ImportJob job) throws MasterNotRunningException, ZooKeeperConnectionException {
        this.workerInfo = workerInfo;
        this.job = job;
        this.admin = new HBaseAdmin(job.getConfig());
    }

    @Override
    public void run() {
        List<Put> dataPuts = new ArrayList<Put>();
        for(Long uid: workerInfo.getData().keySet()){
            String value = workerInfo.getData().get(uid).toString();
            byte[] uidBytes = Bytes.toBytes(uid);
            byte[] shortenUid = {uidBytes[3],uidBytes[4], uidBytes[5], uidBytes[6], uidBytes[7]};
            Put dataPut = new Put(shortenUid);
            dataPut.setWriteToWAL(false);
            if(workerInfo.getProperty().endsWith("_time")){
                dataPut.add(Bytes.toBytes("value"), Bytes.toBytes("value"), Bytes.toBytes(Long.parseLong(value)));
            } else {
                dataPut.add(Bytes.toBytes("value"), Bytes.toBytes("value"), Bytes.toBytes(value));
            }
            dataPuts.add(dataPut);
        }
        try{
            new HTable(workerInfo.getConfig(), "property_" + workerInfo.getTableName() + "_" + workerInfo.getPropertyID()).put(dataPuts);
        } catch (IOException e){
            e.printStackTrace();
        } finally {
            LOG.info("finished: #" + job.finishedCount.addAndGet(dataPuts.size()) + ", total: " + job.totalCount);
        }
    }
}
