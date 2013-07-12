package com.xingcloud.xa.secondaryindex;

import com.xingcloud.xa.secondaryindex.manager.HBaseResourceManager;
import com.xingcloud.xa.secondaryindex.model.Index;
import com.xingcloud.xa.secondaryindex.pool.ThreadPool;
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
import java.util.concurrent.FutureTask;

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
        long s1 = System.nanoTime();
        try {
          HTableAdmin.checkTable(tableName, Constants.columnFamily); // check if table is exist, if not create it
          
          List<List<Mutation>> result = optimizePuts(indexes);
          LOG.info(tableName + " Group: " + result.size() + "\tOptimize taken: " + (System.nanoTime()-s1)/1.0e9 + " sec");

          long s2 = System.nanoTime();
          List<FutureTask> futures = new ArrayList<FutureTask>();
          for (List<Mutation> operations : result) {
            HBaseOperationTask operationTask = new HBaseOperationTask(tableName, operations);
            FutureTask future = ThreadPool.getInstance().addInnerTask(operationTask);
            futures.add(future);
          }

          //Wait for all put tasks finish
          for (FutureTask f : futures) {
            if (f.get() != null) {
              int state = (Integer)f.get();
            }
          }
           
          LOG.info(tableName + " " + tableName + " Put size:" + indexes.size() +
            " completed tablename is " + tableName + " using "
            + (System.nanoTime()-s2)/1.0e9 + " sec");
        } catch (IOException e) {
          if (e.getMessage().contains("interrupted")) {
            throw e;
          }
          LOG.error(tableName + "\t" + e.getMessage(), e);
        }

    }catch (Exception e){
      LOG.error(e.getMessage(), e);
    }
  }
  
  private List<List<Mutation>> optimizePuts(List<Index> indexes) {
 
      List<List<Mutation>> result = new ArrayList<List<Mutation>>();
      List<Mutation> mutations = new ArrayList<Mutation>(2000);
      result.add(mutations);
      try{
        Map<Index, Integer> combineMap = new HashMap<Index, Integer>();

        for(Index index: indexes){
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

        int currentSize = 0;
        int i = 0;
        for(Map.Entry<Index, Integer> entry:combineMap.entrySet()){
          Index index = entry.getKey();
          int operation = entry.getValue();
          if (operation == 0) {
              continue;
          }

          byte[] row = WriteUtils.getUIIndexRowKey(index.getPropertyID(), index.getDate(), index.getValue());

          Mutation mutation = null;
          if(0 > entry.getValue()){
            mutation = new Delete(row);
            ((Delete)mutation).deleteColumns(Constants.columnFamily.getBytes(), WriteUtils.getFiveByte(index.getUid()));
          }else if (0 < entry.getValue()){
            mutation = new Put(row);
            ((Put)mutation).add(Constants.columnFamily.getBytes(), WriteUtils.getFiveByte(index.getUid()),Bytes.toBytes("0"));
          }

          if (mutation != null) {
              mutation.setDurability(Durability.SKIP_WAL);
              if (currentSize < 2000) {
                List<Mutation> operations = result.get(i);
                operations.add(mutation);
                currentSize++;
              } else {
                List<Mutation> operations = new ArrayList<Mutation>();
                operations.add(mutation);
                result.add(operations);
                i++;
                currentSize = 0;
              }
          }
        }
     }catch(Exception e){
        e.printStackTrace(); 
     }
    return result;
  }
}
