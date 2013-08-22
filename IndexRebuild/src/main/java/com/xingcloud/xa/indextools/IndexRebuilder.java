package com.xingcloud.xa.indextools;

import com.xingcloud.userprops_meta_util.UserProp;
import com.xingcloud.userprops_meta_util.UserProps_DEU_Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-8-21
 * Time: 上午10:09
 * To change this template use File | Settings | File Templates.
 */
public class IndexRebuilder {
  private static Log LOG = LogFactory.getLog(IndexRebuilder.class);

  private static final int BATCH_SIZE = 5000;
  private static final String PROPERTIES = "properties_";
  private static final String INDEX = "_index";
  private static final long WRITE_BUFFER_SIZE = 5242880l;
  private static final int BATCH_PUT_SIZE = 5000;
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("val");
  private static final byte[] ZERO = {0};
  private static final int THREAD_SIZE = 6;

  private List<String> pids;
  private Configuration conf = HBaseConfiguration.create();
  private byte[] startDate;
  private byte[] endDate;

  private ThreadPool pool = new ThreadPool("Index Rebuilder", THREAD_SIZE);

  public IndexRebuilder(List<String> pids, String date) {
    this.pids = pids;
    Pair<byte[], byte[]> pair = getStartEndRowKey(date);
    this.startDate = pair.getFirst();
    this.endDate = pair.getSecond();
  }

  public long runCleanup() {
    long st = System.nanoTime();
    List<FutureTask> futures = new ArrayList<FutureTask>();
    for (String pID : pids) {
      IndexCleanupTask task = new IndexCleanupTask(pID);
      FutureTask future = pool.addTask(task);
      futures.add(future);
    }
    pool.shutDown();
    long total = 0;
    StringBuilder summary = new StringBuilder();
    for (FutureTask f : futures) {
      try {
        if (f.get()!=null) {
          Pair<String, Long> pair = (Pair<String, Long>) f.get();
          summary.append(pair.getFirst()).append(": ").append(pair.getSecond()).append("\n");
          total += pair.getSecond();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      } catch (ExecutionException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
    LOG.info("Cleanup index finish:");
    LOG.info(summary.toString());
    LOG.info("Taken: " + (System.nanoTime()-st)/1.0e9 + " sec");
    return total;
  }

  public long runRebuildIndex() {
    long st = System.nanoTime();
    List<FutureTask> futures = new ArrayList<FutureTask>();
    for (String pID : pids) {
      IndexRebuildTask task = new IndexRebuildTask(pID);
      FutureTask future = pool.addTask(task);
      futures.add(future);
    }
    pool.shutDown();
    StringBuilder summary = new StringBuilder();
    long total = 0;
    for (FutureTask f : futures) {
      try {
        if (f.get()!=null) {
          Pair<String, Long> pair = (Pair<String, Long>) f.get();
          summary.append(pair.getFirst()).append(": ").append(pair.getSecond()).append("\n");
          total += pair.getSecond();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      } catch (ExecutionException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
    LOG.info("Rebuild index finish:");
    LOG.info(summary.toString());
    LOG.info("Taken: " + (System.nanoTime()-st)/1.0e9 + " sec");
    return total;
  }

  public static String getPropTableName(String pID) {
    return PROPERTIES + pID;
  }

  public static String getIndexTableName(String pID) {
    if (pID.equals("rebuild-test")) {
      return "p_" + pID + "+i";
    }
    return PROPERTIES + pID + INDEX;
  }

  public Pair<byte[], byte[]> getStartEndRowKey(String date) {
    Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>();
    TimeZone TZ = TimeZone.getTimeZone("GMT+8");
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    df.setTimeZone(TZ);

    try {
      Date temp = df.parse(date);
      Calendar ca = Calendar.getInstance(TZ);
      ca.setTime(temp);
      ca.add(Calendar.DAY_OF_MONTH, 1);
      String nextDate = df.format(ca.getTime());
      byte[] startRow = Bytes.toBytes(Integer.parseInt(date.replace("-", "")));
      byte[] endRow = Bytes.toBytes(Integer.parseInt(nextDate.replace("-", "")));
      pair.setFirst(startRow);
      pair.setSecond(endRow);
    } catch (ParseException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    return pair;
  }

  public static byte[] bytesCombine(byte[]... bytesArrays){
    int length = 0;
    for (byte[] bytes: bytesArrays){
      length += bytes.length;
    }
    byte[] combinedBytes = new byte[length];
    int index = 0;
    for (byte[] bytes: bytesArrays){
      for(byte b: bytes){
        combinedBytes[index] = b;
        index++;
      }
    }
    return combinedBytes;
  }

  public static byte[] getFiveBytes(long suid) {
    byte[] rk = new byte[5];
    rk[0] = (byte) (suid>>32 & 0xff);
    rk[1] = (byte) (suid>>24 & 0xff);
    rk[2] = (byte) (suid>>16 & 0xff);
    rk[3] = (byte) (suid>>8 & 0xff);
    rk[4] = (byte) (suid & 0xff);
    return rk;
  }

  public static List<String> getAllPid() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    List<String> pids = new ArrayList<String>();
    HTableDescriptor descriptors[] =  admin.listTables();
    for (HTableDescriptor desc : descriptors) {
      String name = desc.getNameAsString();
      if (name.startsWith(PROPERTIES) && name.endsWith(INDEX)) {
        String[] fields = name.split("_");
        pids.add(fields[1]);
      }
    }
    return pids;
  }

  protected class IndexRebuildTask implements Callable<Pair<String, Long>> {

    private String pID;

    public IndexRebuildTask(String pID) {
      this.pID = pID;
    }

    @Override
    public Pair<String, Long> call() throws Exception {
      long st = System.nanoTime();
      HTable propTable = null;
      HTable indexTable = null;
      long putCounter = 0;

      try {
        String propTableName = getPropTableName(pID);
        propTable = new HTable(conf, propTableName);

        String indexTableName = getIndexTableName(pID);
        indexTable = new HTable(conf, indexTableName);
        indexTable.setAutoFlush(false);
        indexTable.setWriteBufferSize(WRITE_BUFFER_SIZE);

        List<Put> puts = new ArrayList<Put>();

        Scan scan = new Scan();
        scan.setStartRow(startDate);
        scan.setStopRow(endDate);
        scan.setBatch(BATCH_SIZE);
        scan.setCacheBlocks(false);

        ResultScanner rs = propTable.getScanner(scan);

        for (Result r : rs) {
          KeyValue[] kvs = r.raw();
          for (KeyValue kv : kvs) {
            byte[] row = kv.getRow();
            byte[] qualifier = kv.getQualifier();
            byte[] val = kv.getValue();

            byte[] indexRK = bytesCombine(qualifier, startDate, val);
            byte[] uid = Arrays.copyOfRange(row, 4, row.length);

            Put put = new Put(indexRK);
            put.add(COLUMN_FAMILY, uid, ZERO);
            puts.add(put);
            putCounter++;
            if ((putCounter % BATCH_PUT_SIZE)==0) {
              indexTable.put(puts);
              indexTable.flushCommits();
              puts.clear();
            }
          }
        }

        if (!puts.isEmpty()) {
          indexTable.put(puts);
          indexTable.flushCommits();
          puts.clear();
        }
      } finally {
        if (indexTable != null) {
          indexTable.close();
        }
        if (propTable != null) {
          propTable.close();
        }
      }

      LOG.info("Build " + pID + "'s index table finish. Taken: " + (System.nanoTime()-st)/1.0e9 + " sec");
      return new Pair<String, Long>(pID, putCounter);
    }
  }

  protected class IndexCleanupTask implements Callable<Pair<String, Long>> {

    private String pID;

    public IndexCleanupTask(String pID) {
      this.pID = pID;
    }

    @Override
    public Pair<String, Long> call() throws Exception {
      long st = System.nanoTime();
      List<UserProp> props = UserProps_DEU_Util.getInstance().getUserProps(pID);
      List<Short> ids = new ArrayList<Short>();
      for (UserProp up : props) {
        short id = (short)up.getId();
        ids.add(id);
      }
      String indexTableName = getIndexTableName(pID);
      HTable indexTable = new HTable(conf, indexTableName);
      indexTable.setAutoFlush(false);
      indexTable.setWriteBufferSize(WRITE_BUFFER_SIZE);
      //Delete
      long delCounter = 0l;
      List<Delete> deletes = new ArrayList<Delete>();
      for (short id : ids) {
        byte[] startRow = bytesCombine(Bytes.toBytes(id), startDate);
        byte[] endRow = bytesCombine(Bytes.toBytes(id), endDate);
        Scan scan = new Scan();
        scan.setStartRow(startRow);
        scan.setStopRow(endRow);
        scan.setCacheBlocks(false);
        scan.setBatch(BATCH_SIZE);
        ResultScanner scanner = indexTable.getScanner(scan);
        for (Result r : scanner) {
          byte[] row = r.getRow();
          Delete delete = new Delete(row);
          deletes.add(delete);
          if ((delCounter%BATCH_PUT_SIZE)==0) {
            indexTable.delete(deletes);
            indexTable.flushCommits();
            deletes.clear();
          }
        }
        if (!deletes.isEmpty()) {
          indexTable.delete(deletes);
          indexTable.flushCommits();
          deletes.clear();
        }
      }
      LOG.info("Cleanup " + pID + "'s index table finish. Taken: " + (System.nanoTime()-st)/1.0e9 + " sec");
      return new Pair<String, Long>(pID, delCounter);
    }
  }

  public static void main(String[] args) {
    String mode = args[0];
    String date = args[1];
    String pid = args[2];

    List<String> pids = new ArrayList<String>();
    if (pid.equals("all")) {
      try {
        pids = getAllPid();
      } catch (IOException e) {
        e.printStackTrace();
        LOG.error(e.getMessage());
      }
    } else {
       pids.add(pid);
    }

    IndexRebuilder ir = new IndexRebuilder(pids, date);
    if (mode.equals("cleanup")) {
      ir.runCleanup();
    } else if (mode.equals("build")) {
      ir.runRebuildIndex();
    } else if (mode.equals("rebuild")) {
      ir.runCleanup();
      ir.runRebuildIndex();
    }
  }

}
