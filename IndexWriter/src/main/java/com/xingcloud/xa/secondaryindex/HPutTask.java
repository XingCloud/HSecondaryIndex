package com.xingcloud.xa.secondaryindex;

import com.xingcloud.xa.secondaryindex.model.Index;
import com.xingcloud.xa.secondaryindex.model.IndexIgnoreOperationComparator;
import com.xingcloud.xa.secondaryindex.pool.ThreadPool;
import com.xingcloud.xa.secondaryindex.utils.Constants;
import com.xingcloud.xa.secondaryindex.utils.HTableAdmin;
import com.xingcloud.xa.secondaryindex.utils.WriteUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
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

  private static final int PUT_SIZE = 4000;
  
  public HPutTask(String tableName, List<Index> indexes){
    
    this.tableName = tableName;
    this.indexes = indexes;
  }
  
  @Override
  public void run() {
    try{
        try {
          HTableAdmin.checkTable(tableName, Constants.COLUMN_FAMILY); // check if table is exist, if not create it

		  long s1 = System.nanoTime();
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
      List<Mutation> mutations = new ArrayList<Mutation>(PUT_SIZE);
      result.add(mutations);

      Collections.sort(indexes, new IndexIgnoreOperationComparator());
      IndexIgnoreOperationComparator comparator = new IndexIgnoreOperationComparator();

      int currentSize = 0;  // track current size of the group
      int group = 0;    // group number

      for (int outer = 0; outer < indexes.size(); ){
          Index startIndex = indexes.get(outer);
          int inner = outer + 1;
          // traverse forward until encounter one that's not equal to startIndex
          for ( ; inner < indexes.size(); inner++){
              Index nextIndex = indexes.get(inner);
              if (comparator.compare(startIndex, nextIndex) != 0){
                  break;
              }
          }

          int sum = 0;
          for (int i = outer; i < inner; i++){
              sum += indexes.get(i).getOperation().equals(Constants.OPERATION_DELETE) ? -1 : 1;
          }

          if (sum != 0){
              byte[] row = WriteUtils.getUIIndexRowKey(
                      startIndex.getPropertyID(), startIndex.getDate(), startIndex.getValue());

              Mutation mutation = null;
              if (sum < 0){
                  mutation = new Delete(row);
                  ((Delete)mutation).deleteColumns(Constants.COLUMN_FAMILY.getBytes(),
                          WriteUtils.getFiveBytes(startIndex.getUid()));
              } else {
                  mutation = new Put(row);
                  ((Put)mutation).add(Constants.COLUMN_FAMILY.getBytes(),
                          WriteUtils.getFiveBytes(startIndex.getUid()), Bytes.toBytes('0'));
              }

              mutation.setDurability(Durability.SKIP_WAL);
              if (currentSize < PUT_SIZE) {
                  List<Mutation> operations = result.get(group);
                  operations.add(mutation);
                  currentSize++;
              } else {
                  List<Mutation> operations = new ArrayList<Mutation>(PUT_SIZE);
                  operations.add(mutation);
                  result.add(operations);
                  group++;
                  currentSize = 1;
              }
          }

          outer = inner;    // skip those equal indexes
      }

      return result;
  }
}
