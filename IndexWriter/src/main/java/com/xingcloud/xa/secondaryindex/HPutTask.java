package com.xingcloud.xa.secondaryindex;

import com.xingcloud.xa.secondaryindex.model.Index;
import com.xingcloud.xa.secondaryindex.utils.Constants;
import com.xingcloud.xa.secondaryindex.utils.HTableAdmin;
import com.xingcloud.xa.secondaryindex.utils.WriteUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 5/17/13
 * Time: 10:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class HPutTask implements Runnable  {
  private static final Log LOG = LogFactory.getLog(SecondaryIndexWriter.class); 
  private String tableName;
  private List<Index> indexes; //key=>every hbase node, value=>put and delete index
  
  public HPutTask(String tableName, List<Index> indexes){
    
    this.tableName = tableName;
    this.indexes = indexes;
  }
  
  @Override
  public void run() {
    try{
        boolean putHbase = true;
        while (putHbase) {
        HTable table = null;
        long currentTime = System.currentTimeMillis();
        try {
          HTableAdmin.checkTable(tableName, Constants.columnFamily); // check if table is exist, if not create it
          
          table = new HTable(HTableAdmin.getHBaseConf(), tableName);
          LOG.info(tableName + " init htable .." + currentTime);
          table.setAutoFlush(false);
          table.setWriteBufferSize(Constants.WRITE_BUFFER_SIZE);
          
          Pair<List<Delete>, List<Put>> deletePut = optimizePuts(indexes);
          LOG.info("Delete siz: " + deletePut.getFirst().size() + "\tPut size: " + deletePut.getSecond().size());

          table.delete(deletePut.getFirst());//todo wcl batch
          table.put(deletePut.getSecond());
          
          table.flushCommits();
           
          putHbase = false;
          LOG.info(tableName + " " + tableName + " Put size:" + indexes.size() +
            " completed tablename is " + tableName + " using "
            + (System.currentTimeMillis() - currentTime) + "ms");
        } catch (IOException e) {
          if (e.getMessage().contains("interrupted")) {
            throw e;
          }
          LOG.error(tableName + tableName + e.getMessage(), e);
          if (e.getMessage().contains("HConnectionImplementation") && e.getMessage().contains("closed")) {
            HConnectionManager.deleteConnection(HTableAdmin.getHBaseConf());
          }
          putHbase = true;

          LOG.info("trying put hbase " + tableName + " " + tableName + "again...tablename " +
            ":" + tableName);
          Thread.sleep(5000);
        } finally {
          try {
            if (table != null) {
              table.close();
              LOG.info(tableName + " close this htable." + currentTime);
            }
          } catch (IOException e) {
            LOG.error(tableName + e.getMessage(), e);
          }
        }
      }
    }catch (Exception e){
      e.printStackTrace();  
    }
  }
  
  private Pair<List<Delete>, List<Put>> optimizePuts(List<Index> indexes){
 
      Pair<List<Delete>, List<Put>> result = new Pair<List<Delete>, List<Put>>();
      try{
        result.setFirst(new ArrayList<Delete>());
        result.setSecond(new ArrayList<Put>());

        Map<Index, Integer> combineMap = new HashMap<Index, Integer>();

        for(Index index: indexes){
          LOG.info("Process " + index);
          int operation = 1;
          if(index.getOperation().equals("delete")){
            operation = -1;
          }

          Integer indexType = combineMap.get(index);
          if (indexType == null) {
              combineMap.put(index, operation);
          } else {
              combineMap.put(index, operation+indexType);
          }
        }
        
        for(Map.Entry<Index, Integer> entry:combineMap.entrySet()){
          Index index = entry.getKey();
          int operation = entry.getValue();
          if (operation == 0) {
              continue;
          }

          byte[] row = WriteUtils.getUIIndexRowKey(index.getPropertyID(), index.getDate(), index.getValue());
          if(0 < entry.getValue()){
            Delete delete = new Delete(row);
            delete.deleteColumns(Constants.columnFamily.getBytes(), WriteUtils.getFiveByte(index.getUid()));
            delete.setDurability(Durability.SKIP_WAL);
            result.getFirst().add(delete);
          }else if (0 > entry.getValue()){
            Put put = new Put(row);
            put.setDurability(Durability.SKIP_WAL);
            put.add(Constants.columnFamily.getBytes(), WriteUtils.getFiveByte(index.getUid()),Bytes.toBytes("0"));
            result.getSecond().add(put);
          }
        }
     }catch(Exception e){
        e.printStackTrace(); 
     }
    return result;
  }
}
