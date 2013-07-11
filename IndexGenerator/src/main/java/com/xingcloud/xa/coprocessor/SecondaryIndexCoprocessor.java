package com.xingcloud.xa.coprocessor;

import com.google.protobuf.ServiceException;
import com.xingcloud.userprops_meta_util.UpdateFunc;
import com.xingcloud.userprops_meta_util.UserProp;
import com.xingcloud.userprops_meta_util.UserProps_DEU_Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HRegionServerRegister;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Jian Fang
 * Date: 13-5-13
 * Time: 下午2:28
 */
public class SecondaryIndexCoprocessor extends BaseRegionObserver {

    private static Map<String, Map<Integer, UpdateFunc>> metaInfo = new ConcurrentHashMap<String, Map<Integer, UpdateFunc>>();
    private static Log INDEX_LOG = LogFactory.getLog(SecondaryIndexCoprocessor.class);
    private static Log LOG = LogFactory.getLog(HRegionServer.class);

    private static final byte[] CF_NAME = Bytes.toBytes("value");


    @Override
    public void start(CoprocessorEnvironment e){
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

        if(!tableName.startsWith("properties_") || tableName.endsWith("_index")){
            return;
        }

        String[] fields = tableName.split("_");
        String projectID = fields[1];

        HTableInterface dataTable = observerContext.getEnvironment().getTable(table);

        List<byte[]> qualifierList = new ArrayList<byte[]>();
        //Cache KV in puts
        Map<Integer, KeyValue> kvCache = new HashMap<Integer, KeyValue>();
        //Cache index in puts
        Map<Integer, Integer> indexCache = new HashMap<Integer, Integer>();

        List<? extends Cell> cells = put.getFamilyMap().get(CF_NAME);

        int i = 0;
        for (Cell cell : cells) {
            KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
            byte[] qualifier = kv.getQualifier();
            qualifierList.add(qualifier);
            kvCache.put(Bytes.toInt(qualifier), kv);
            indexCache.put(Bytes.toInt(qualifier), i);
            i++;
        }

        //Get old values which related to the qualifier
        KeyValue[] oldValues = null;
        try {
            oldValues = getValue(observerContext.getEnvironment().getRegion().getRegionName(), put.getRow(), qualifierList);
        } catch (ServiceException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            return;
        }

        Map<Integer, UpdateFunc> metaMap = getMetaInfo(projectID);

        //Put attributes which already exist in table
        if (oldValues != null) {
            for (KeyValue kvOld : oldValues) {
                int qualifier = Bytes.toInt(kvOld.getQualifier());
                byte[] oldValue = kvOld.getValue();
                KeyValue kvNew = kvCache.get(qualifier);
                byte[] newValue = kvNew.getValue();
                long ts = kvNew.getTimestamp();

                UpdateFunc uf = metaMap.get(qualifier);
                if (uf == null) {
                    //Reload meta info
                    metaMap = getMetaInfo(projectID);
                    uf = metaMap.get(qualifier);
                    if (uf == null) {
                        LOG.error("Attribute: [" + qualifier + "] doesn't exist in meta table!");
                        return;
                    }
                }
                if (!Bytes.equals(oldValue, newValue)) {
                     //Update attribute value and ignore once
                     if (uf == UpdateFunc.cover) {
                        submitIndexJob(projectID, true, put.getRow(), qualifier, oldValue, newValue, ts);
                     }
                } else if (uf == UpdateFunc.inc) {
                     //Increment attribute value
                    int index = indexCache.get(qualifier);
                    cells.remove(index);
                    if (ts > kvOld.getTimestamp()) {
                        byte[] result = Bytes.toBytes(Bytes.toLong(oldValue) + Bytes.toLong(newValue));
                        submitIndexJob(projectID, true, put.getRow(), qualifier, oldValue, result, ts);
                        //Increment val and put to table
                        put.add(CF_NAME, Bytes.toBytes(qualifier), ts, result);
                    }
                }
                kvCache.remove(qualifier);
            }
        }

        //Put remain attributes which don't exist in table before
        for (Map.Entry<Integer, KeyValue> entry : kvCache.entrySet()) {
            int qualifier = entry.getKey();
            KeyValue kv = entry.getValue();
            UpdateFunc uf = metaMap.get(qualifier);
            long ts = kv.getTimestamp();
            if (uf == UpdateFunc.once) {
                ts = Long.MAX_VALUE - ts;
            }
            submitIndexJob(projectID, false, put.getRow(), qualifier, null, kv.getValue(), ts);
        }

        dataTable.close();
    }

    private KeyValue[] getValue(byte[] regionName, byte[] uid, List<byte[]> qualifierList) throws IOException, ServiceException {
        Get get = new Get(uid);
        for (byte[] qualifier : qualifierList) {
            get.addColumn(CF_NAME, qualifier);
        }
        ClientProtos.GetRequest request = RequestConverter.buildGetRequest(regionName, get);
        ClientProtos.GetResponse response = HRegionServerRegister.getLast().get(null, request);

        if (response == null) return null;
        Result result = ProtobufUtil.toResult(response.getResult());

        if(result.isEmpty()){
            return null;
        } else {
            return result.raw();
        }
    }

    private void submitIndexJob(String projectID, boolean shouldDel, byte[] uid,
                                int propertyID, byte[] oldValue, byte[] newValue, long ts) throws IOException {
        byte[] convertedUid = {0,0,0,0,0,0,0,0};
        for(int i = 0; i < 5; i++)
            convertedUid[i+3] = uid[i];

        long uidL = Bytes.toLong(convertedUid);
        String oldValueStr = Bytes.toStringBinary(oldValue);
        String newValueStr = Bytes.toStringBinary(newValue);

        INDEX_LOG.info(ts + "\t" + uidL + "\t" + propertyID + "\t" + oldValueStr + "\t" + newValueStr + "\t" + shouldDel + "\t" + projectID);
    }

    private Map<Integer, UpdateFunc> getMetaInfo(String projectID) throws IOException {
        Map<Integer, UpdateFunc> metaMap = metaInfo.get(projectID);
        if (metaMap == null) {
            long st = System.nanoTime();
            List<UserProp> props = UserProps_DEU_Util.getInstance().getUserProps(projectID);
            LOG.info("Scan property table finished. Property number: " +  props.size() + "Taken: " + (System.nanoTime()-st)/1.0e9 + " sec");
            metaMap = new HashMap<Integer, UpdateFunc>();
            for (UserProp up : props) {
                int id = up.getId();
                UpdateFunc uf = up.getPropFunc();
                metaMap.put(id, uf);
            }
            metaInfo.put(projectID, metaMap);
        }
        return metaMap;
    }


}
