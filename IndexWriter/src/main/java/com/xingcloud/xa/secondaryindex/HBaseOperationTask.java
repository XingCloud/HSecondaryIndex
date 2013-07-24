package com.xingcloud.xa.secondaryindex;

import com.xingcloud.xa.secondaryindex.manager.HBaseResourceManager;
import com.xingcloud.xa.secondaryindex.utils.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-7-11
 * Time: 下午7:26
 * To change this template use File | Settings | File Templates.
 */
public class HBaseOperationTask implements Callable<Integer>{
    private static Log LOG = LogFactory.getLog(HBaseOperationTask.class);

    private static final long SLEEP_TIME = 60*1000;
    private List<Mutation> operations;
    private String tableName;

    public HBaseOperationTask(String tableName, List<Mutation> operations) {
        this.operations = operations;
        this.tableName = tableName;
    }

    @Override
    public Integer call() {
        boolean successful = true;
        long tryNum = 0;
        while (true) {
          long st = System.nanoTime();
          HTableInterface ht = null;
          try {
              ht = HBaseResourceManager.getInstance().getTable(tableName);
              ht.setWriteBufferSize(Constants.WRITE_BUFFER_SIZE);
              ht.batch(operations);
              LOG.info(tableName + " put " + operations.size() + " records. Taken: " + (System.nanoTime()-st)/1.0e9 + " sec");
          } catch (Exception e) {
              LOG.error(e.getMessage() + "\n" + e.getStackTrace().toString());
              successful = false;
            try {
              LOG.info("HBase is not available. Sleep and retry... Try number: " + ++tryNum);
              Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e1) {
              LOG.error("Sleep interrupted! Index update should redo.");
              return -1;
            }
          } finally {
              try {
                  HBaseResourceManager.getInstance().putTable(ht);
              } catch (IOException e) {
                  LOG.error(e.getMessage(), e);
              }
          }
          if (successful) {
            break;
          }
        }
        return 1;
    }
}
