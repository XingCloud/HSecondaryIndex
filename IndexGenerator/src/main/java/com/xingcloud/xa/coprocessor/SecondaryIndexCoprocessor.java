package com.xingcloud.xa.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegionServerRegister;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Jian Fang
 * Date: 13-5-13
 * Time: 下午2:28
 */
public class SecondaryIndexCoprocessor extends BaseRegionObserver {
    private HTablePool pool;
    private ObjectMapper mapper;

    private static Log LOG = LogFactory.getLog(SecondaryIndexCoprocessor.class);

    @Override
    public void start(CoprocessorEnvironment e){
        mapper = new ObjectMapper();
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
        List<KeyValue> values = put.get(Bytes.toBytes("value"), Bytes.toBytes("value"));
        byte[] newValue = {};
        if(values.size() > 0) newValue = values.get(0).getValue();
        byte[] oldValue = getValue(observerContext.getEnvironment().getRegion().getRegionName(), dataTable, put.getRow());

        long s2 = System.nanoTime();
        if(oldValue == null){
            submitIndexJob(projectID, false, put.getRow(), propertyID, null, newValue);
        } else if(!Bytes.equals(oldValue, newValue)){
            submitIndexJob(projectID, true, put.getRow(), propertyID, oldValue, newValue);
        }
        long s3 = System.nanoTime();
        dataTable.close();
    }

    private byte[] getValue(byte[] regionName, HTableInterface table, byte[] uid) throws IOException {
        Get get = new Get(uid);
        Result result = HRegionServerRegister.getLast().get(regionName, get);
        //Result result = table.get(get);
        if(result.isEmpty()){
            return null;
        } else {
            return result.getValue(Bytes.toBytes("value"), Bytes.toBytes("value"));
        }
    }

    private void submitIndexJob(String projectID, boolean shouldDel, byte[] uid,
                                int propertyID, byte[] oldValue, byte[] newValue) throws IOException {
        byte[] convertedUid = {0,0,0,0,0,0,0,0};
        for(int i = 0; i < 5; i++)
            convertedUid[i+3] = uid[i];
        Map<String, Object> jobMap = new HashMap<String, Object>();
        jobMap.put("uid", Bytes.toLong(convertedUid));
        jobMap.put("propertyID", propertyID);
        jobMap.put("old_value", Bytes.toStringBinary(oldValue));
        jobMap.put("new_value", Bytes.toStringBinary(newValue));
        jobMap.put("delete", shouldDel);
        jobMap.put("pid", projectID);
        LOG.info(mapper.writeValueAsString(jobMap));
    }

}
