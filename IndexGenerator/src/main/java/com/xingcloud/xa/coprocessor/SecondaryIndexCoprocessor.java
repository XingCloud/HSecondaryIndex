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
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Jian Fang
 * Date: 13-5-13
 * Time: 下午2:28
 */
public class SecondaryIndexCoprocessor extends BaseRegionObserver {
    private HTablePool pool;
    private ObjectMapper mapper;

    private static Map<String, Map<Integer, UpdateFunc>> metaInfo = new ConcurrentHashMap<String, Map<Integer, UpdateFunc>>();
    private static Log INDEX_LOG = LogFactory.getLog(SecondaryIndexCoprocessor.class);
    private static Log LOG = LogFactory.getLog(HRegionServer.class);

    private static final byte[] CF_NAME = Bytes.toBytes("value");

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

        if(!tableName.startsWith("properties_") || tableName.endsWith("_index")){
            return;
        }

        String[] fields = tableName.split("_");
        String projectID = fields[1];

        HTableInterface dataTable = observerContext.getEnvironment().getTable(table);

        List<byte[]> qualifierList = new ArrayList<byte[]>();
        Map<Integer, byte[]> cache = new HashMap<Integer, byte[]>();

        for (Cell cell : put.getFamilyMap().get(CF_NAME)) {
            KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
            byte[] qualifier = kv.getQualifier();
            qualifierList.add(qualifier);
            cache.put(Bytes.toInt(qualifier), kv.getValue());
        }

        LOG.info("Num of puts for one row: " + cache.size());

        //Get old values which related to the qualifier
        KeyValue[] oldValues = null;
        try {
            long s1 = System.nanoTime();
            oldValues = getValue(observerContext.getEnvironment().getRegion().getRegionName(), put.getRow(), qualifierList);
            int size = oldValues == null ? 0 : oldValues.length;
            LOG.info("Old value size: " + size + " Taken: " + (System.nanoTime()-s1)/1.0e9 + " sec");
        } catch (ServiceException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            return;
        }

        Map<Integer, UpdateFunc> metaMap = getMetaInfo(projectID);

        //Put attributes which already exist in table
        if (oldValues != null) {
            for (KeyValue kv : oldValues) {
                int qualifier = Bytes.toInt(kv.getQualifier());
                byte[] oldValue = kv.getValue();
                byte[] newValue = cache.get(qualifier);

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
                        submitIndexJob(projectID, true, put.getRow(), qualifier, oldValue, newValue);
                     }
                } else if (uf == UpdateFunc.inc) {
                     //Increment attribute value
                     byte[] result = Bytes.toBytes(Bytes.toLong(oldValue) + Bytes.toLong(newValue));
                     submitIndexJob(projectID, true, put.getRow(), qualifier, oldValue, result);
                }
                cache.remove(qualifier);
            }
        }

        //Put remain attributes which don't exist in table before
        for (Map.Entry<Integer, byte[]> entry : cache.entrySet()) {
            int qualifier = entry.getKey();
            byte[] val = entry.getValue();
            submitIndexJob(projectID, false, put.getRow(), qualifier, null, val);
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
        INDEX_LOG.info(mapper.writeValueAsString(jobMap));
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
