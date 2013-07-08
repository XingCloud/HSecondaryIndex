package com.xingcloud.xa.coprocessor;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.HRegionServerRegister;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * User: Jian Fang
 * Date: 13-5-13
 * Time: 下午2:28
 */
public class SecondaryIndexCoprocessor extends BaseRegionObserver {
    private HTablePool pool;
    private ObjectMapper mapper;

    private Map<String, Map<String, Integer>> properties = new HashMap<String, Map<String, Integer>>();
  
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
            final Durability durability)
            throws IOException {
        byte[] table  = observerContext.getEnvironment().getRegion().getRegionInfo().getTableName();
        String tableName = Bytes.toString(table);

        if(!tableName.startsWith("property_") || tableName.endsWith("_index")){
            return;
        }
//        int index = tableName.lastIndexOf("_");
//        String projectID = tableName.substring(tableName.indexOf("_")+1, index);//sof-dsk
//        int propertyID = Integer.parseInt(tableName.substring(index + 1));

        String[] fields = tableName.split("_");
        String projectID = fields[1];
        int propertyID = Integer.parseInt(fields[2]);
        String updateFunc = fields[3];


        long s1 = System.nanoTime();
        HTableInterface dataTable = observerContext.getEnvironment().getTable(table);
        List<KeyValue> values = put.get(Bytes.toBytes("value"), Bytes.toBytes("value"));
        byte[] newValue = {};
        if(values.size() > 0) newValue = values.get(0).getValue();
        byte[] oldValue = new byte[0];
        try {
            oldValue = getValue(observerContext.getEnvironment().getRegion().getRegionName(), put.getRow());
        } catch (ServiceException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            return;
        }

        long s2 = System.nanoTime();
        if(oldValue == null){
            submitIndexJob(projectID, false, put.getRow(), propertyID, null, newValue);
        } else if(!Bytes.equals(oldValue, newValue)){
            if (updateFunc.equals("c")) {
                //Cover update manner
                submitIndexJob(projectID, true, put.getRow(), propertyID, oldValue, newValue);
            }
        }
        long s3 = System.nanoTime();
        dataTable.close();
    }

    @Override
    public Result postIncrement(final ObserverContext<RegionCoprocessorEnvironment> observerContext,
                                final Increment increment, final Result result) throws IOException {
        byte[] table  = observerContext.getEnvironment().getRegion().getRegionInfo().getTableName();
        String tableName = Bytes.toString(table);
        if(!tableName.startsWith("property_") || tableName.endsWith("_index")){
            return result;
        }

        String[] fields = tableName.split("_");
        String projectID = fields[1];
        int propertyID = Integer.parseInt(fields[2]);

        Map<byte[], NavigableMap<byte [], Long>> resultMap = increment.getFamilyMapOfLongs();
        Map<byte[], Long> specificCol = resultMap.get(Bytes.toBytes("value"));

        for (KeyValue kv : result.raw()) {
            byte[] q = kv.getQualifier();
            long incrementVal = specificCol.get(q);
            long resultVal = Bytes.toLong(kv.getValue());
            if (incrementVal == resultVal) {
                submitIndexJob(projectID, false, kv.getRow(), propertyID, null, Bytes.toBytes(resultVal));
            } else {
                submitIndexJob(projectID, true, kv.getRow(), propertyID, Bytes.toBytes(resultVal-incrementVal), Bytes.toBytes(resultVal));
            }
        }
        return result;
    }


    private byte[] getValue(byte[] regionName, byte[] uid) throws IOException, ServiceException {
        Get get = new Get(uid);
        ClientProtos.GetRequest request = RequestConverter.buildGetRequest(regionName, get);
        ClientProtos.GetResponse response = HRegionServerRegister.getLast().get(null, request);

        if (response == null) return null;
        Result result = ProtobufUtil.toResult(response.getResult());

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
        jobMap.put("timestamp", new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime()));
        jobMap.put("uid", Bytes.toLong(convertedUid));
        jobMap.put("propertyID", propertyID);
        jobMap.put("old_value", Bytes.toStringBinary(oldValue));
        jobMap.put("new_value", Bytes.toStringBinary(newValue));
        jobMap.put("delete", shouldDel);
        jobMap.put("pid", projectID);
        LOG.info(mapper.writeValueAsString(jobMap));
    }



}
