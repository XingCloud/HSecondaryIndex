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
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
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

	// projectID => {propertyID => UpdateFunc, ...}
    private static Map<String, Map<Integer, UpdateFunc>> metaInfo = new ConcurrentHashMap<String, Map<Integer, UpdateFunc>>();
    private static Log INDEX_LOG = LogFactory.getLog(SecondaryIndexCoprocessor.class);
    private static Log LOG = LogFactory.getLog(HRegionServer.class);

    private static final byte[] CF_NAME = Bytes.toBytes("val");
	private static final String PROPERTY_TABLE_PREFIX = "properties_";

    @Override
    public void prePut(
            final ObserverContext<RegionCoprocessorEnvironment> observerContext,
            final Put put,
            final WALEdit edit,
            final Durability durability)
            throws IOException {
        HRegion region = observerContext.getEnvironment().getRegion();
        byte[] table  = region.getRegionInfo().getTableName();

        String tableName = Bytes.toString(table);

        if(!tableName.startsWith(PROPERTY_TABLE_PREFIX) || tableName.endsWith("_index")){
            return;
        }


        String projectID = tableName.substring(PROPERTY_TABLE_PREFIX.length());

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
            oldValues = getValue(region, put.getRow(), qualifierList);
        } catch (IOException e) {
            LOG.error("Update index table got exception! MSG: " + e.getMessage(), e);
            return;
        }

        Map<Integer, UpdateFunc> metaMap = getMetaInfo(projectID, false);

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
                    metaMap = getMetaInfo(projectID, true);
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
                }
				// whether oldValue equals newValue or not
				if (uf == UpdateFunc.inc) {
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
            if (uf == null) {
              //Reload meta info
              metaMap = getMetaInfo(projectID, true);
              uf = metaMap.get(qualifier);
              if (uf == null) {
                LOG.error("Attribute: [" + qualifier + "] doesn't exist in meta table!");
                return;
              }
            }

            long ts = kv.getTimestamp();
            if (uf == UpdateFunc.once) {
                ts = Long.MAX_VALUE - ts;
            }
            submitIndexJob(projectID, false, put.getRow(), qualifier, null, kv.getValue(), ts);
        }

    }

    private KeyValue[] getValue(HRegion region, byte[] uid, List<byte[]> qualifierList) throws IOException {
        Get get = new Get(uid);
        for (byte[] qualifier : qualifierList) {
            get.addColumn(CF_NAME, qualifier);
        }
      Result r = null;
      try {
        r = region.get(get);
      } catch (IOException e) {
          LOG.debug("Get property value got exception. MSG: " + e.getMessage() + "\nTry get from HTable client...");
          HTableInterface table = HBaseResourceManager.getInstance().getTable(region.getTableDesc().getNameAsString());
          try {
            r = table.get(get);
          } finally {
            if (table != null) {
              HBaseResourceManager.getInstance().putTable(table);
            }
          }
        }
      if(r.isEmpty()){
            return null;
        } else {
            return r.raw();
        }
    }

    private void submitIndexJob(String projectID, boolean shouldDel, byte[] uid,
                                int propertyID, byte[] oldValue, byte[] newValue, long ts) throws IOException {
        byte[] convertedUid = {0,0,0,0,0,0,0,0};
		System.arraycopy(uid, 0, convertedUid, 3, 5);

        long uidL = Bytes.toLong(convertedUid);
        String oldValueStr = Bytes.toStringBinary(oldValue);
        String newValueStr = Bytes.toStringBinary(newValue);

        INDEX_LOG.info(ts + "\t" + uidL + "\t" + propertyID + "\t" + oldValueStr + "\t" + newValueStr + "\t" + shouldDel + "\t" + projectID);
    }

    private Map<Integer, UpdateFunc> getMetaInfo(String projectID, boolean force) throws IOException {
        Map<Integer, UpdateFunc> metaMap = metaInfo.get(projectID);
        if (metaMap == null || force) {
            long st = System.nanoTime();
            List<UserProp> props = UserProps_DEU_Util.getInstance().getUserProps(projectID);
            LOG.info("Scan property table finished. Property number: " +  props.size() + "Taken: " + (System.nanoTime()-st)/1.0e9 + " sec");
            metaMap = new HashMap<Integer, UpdateFunc>();
            for (UserProp up : props) {
                metaMap.put(up.getId(), up.getPropFunc());
            }
            metaInfo.put(projectID, metaMap);
        }
        return metaMap;
    }
}
