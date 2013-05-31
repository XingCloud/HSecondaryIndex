package com.xingcloud.xa.secondaryindex;

import com.xingcloud.xa.secondaryindex.model.Index;
import com.xingcloud.xa.secondaryindex.utils.Constants;
import com.xingcloud.xa.secondaryindex.utils.HTableAdmin;
import com.xingcloud.xa.secondaryindex.utils.WriteUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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
  private Map<String,List<Index>> indexs; //key=>every hbase node, value=>put and delete index
  
  public HPutTask(String tableName, Map<String,List<Index>> indexs){
    
    this.tableName = tableName;
    this.indexs = indexs;
  }
  
  @Override
  public void run() {
    try{
    for (Map.Entry<String, List<Index>> entry : indexs.entrySet()) {
      boolean putHbase = true;
        while (putHbase) {
        HTable table = null;
        long currentTime = System.currentTimeMillis();
        try {
          HTableAdmin.checkTable(entry.getKey(), WriteUtils.getUIIndexTableName(tableName), Constants.columnFamily); // check if table is exist, if not create it
          
          table = new HTable(HTableAdmin.getHBaseConf(entry.getKey()), WriteUtils.getUIIndexTableName(tableName));//todo wcl
          LOG.info(tableName + " init htable .." + currentTime);
          table.setAutoFlush(false);
          table.setWriteBufferSize(Constants.WRITE_BUFFER_SIZE);
          
          Pair<List<Delete>, List<Put>> deletePut = optimizePuts(entry.getValue());
          
          table.delete(deletePut.getFirst());
          table.put(deletePut.getSecond());
          
          table.flushCommits();

          putHbase = false;
          LOG.info(tableName + " " + entry.getKey() + " put hbase size:" + entry.getValue().size() +
            " completed tablename is " + tableName + " using "
            + (System.currentTimeMillis() - currentTime) + "ms");
        } catch (IOException e) {
          if (e.getMessage().contains("interrupted")) {
            throw e;
          }
          LOG.error(tableName + entry.getKey() + e.getMessage(), e);
          if (e.getMessage().contains("HConnectionImplementation") && e.getMessage().contains("closed")) {
            HConnectionManager.deleteConnection(HTableAdmin.getHBaseConf(entry.getKey()), true);
            LOG.warn("delete connection to " + entry.getKey());
          }
          putHbase = true;

          LOG.info("trying put hbase " + tableName + " " + entry.getKey() + "again...tablename " +
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
    }   
    }catch (Exception e){
      e.printStackTrace();  
    }
  }
  
  private Pair<List<Delete>, List<Put>> optimizePuts(List<Index> indexs){
 
      Pair<List<Delete>, List<Put>> result = new Pair<List<Delete>, List<Put>>();
      try{
        result.setFirst(new ArrayList<Delete>());
        result.setSecond(new ArrayList<Put>());

        Map<Index, Integer> combineMap = new HashMap<Index, Integer>();
        int operation;
        
        //System.out.println("Before optimize:");
        for(Index index: indexs){
          //System.out.println(index.toString());
          operation = 1;
          if(index.getOperation().equals("delete")){
            operation = -1;
          }
          
          if(!combineMap.containsKey(index)){
            combineMap.put(index, operation);  
          }else{
            int i = combineMap.get(index) + operation;
            if(i>1) i = 1;
            if(i<-1) i = -1;
            combineMap.put(index, i);
          }     
        }
        
        //System.out.println("After optimize:");
        for(Map.Entry<Index, Integer> entry:combineMap.entrySet()){      
          Index index = entry.getKey();
          //if(Math.abs(entry.getValue())==1)System.out.println(index.toString());
    
          byte[] row = WriteUtils.getUIIndexRowKey(index.getPropertyID(), index.getTimestamp(), index.getValue());
          if(-1 == entry.getValue()){
            Delete delete = new Delete(row);
            delete.deleteColumns(Constants.columnFamily.getBytes(), WriteUtils.getFiveByte(index.getUid()));
            delete.setWriteToWAL(Constants.deuTableWalSwitch);
            result.getFirst().add(delete);
          }else if (1 == entry.getValue()){
            Put put = new Put(row);
            put.setWriteToWAL(Constants.deuTableWalSwitch);
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
