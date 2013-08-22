package com.xingcloud.xa.indextools;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-8-21
 * Time: 下午2:32
 * To change this template use File | Settings | File Templates.
 */

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xingcloud.userprops_meta_util.*;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.*;

public class TestIndexREbuilder {
  private static final Log LOG = LogFactory.getLog(TestIndexREbuilder.class);

  private final static String pID = "rebuild-test";
  private final static byte[] FAMILY = Bytes.toBytes("val");
  private final static byte[] date = Bytes.toBytes(19800101);
  private final static byte[] nextDate = Bytes.toBytes(19800102);
  private final static String dateStr = "1980-01-01";
  private static Configuration conf = HBaseConfiguration.create();
  private static String indexTableName = "p_" + pID + "_i";
  private static String propTableName = "p_" + pID;
  private static String metaTableName = "meta_properties";
  private static int UID_NUM = 10;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    List<UserProp> ups = new ArrayList<UserProp>();
    UserProp up0 = new UserProp(0, "register_time", PropType.sql_datetime, UpdateFunc.once, PropOrig.sys,
            "rt", "TOTAL_USER");
    ups.add(up0);

    UserProp up1 = new UserProp(1, "nation", PropType.sql_string, UpdateFunc.cover, PropOrig.sys,
            "na", "TOTAL_USER");
    ups.add(up1);

    UserProp up2 = new UserProp(2, "pay_amount", PropType.sql_bigint, UpdateFunc.inc, PropOrig.sys,
            "pa", "TOTAL_USER");
    ups.add(up2);

    //Init meta table

    HTable metaTable = null;
    try {
      metaTable = new HTable(conf, metaTableName);
      metaTable.setAutoFlush(false);
      List<Put> puts = new ArrayList<Put>();
      for (UserProp up : ups) {
        String name = up.getPropName();
        int id = up.getId();
        byte[] rk = Bytes.toBytes(pID+"_"+name);
        Put put = new Put(rk);
        put.add(FAMILY, Bytes.toBytes("func"), Bytes.toBytes(up.getPropFunc().name()));
        put.add(FAMILY, Bytes.toBytes("id"), Bytes.toBytes(id));
        put.add(FAMILY, Bytes.toBytes("orig"), Bytes.toBytes(up.getPropOrig().name()));
        put.add(FAMILY, Bytes.toBytes("type"), Bytes.toBytes(up.getPropType().name()));
        puts.add(put);
      }
      metaTable.put(puts);
      metaTable.flushCommits();

    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } finally {
      if (metaTable != null) {
        try {
          metaTable.close();
        } catch (IOException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
      }
    }
    //Init tables
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);

      if (admin.tableExists(indexTableName)) {
        admin.disableTable(indexTableName);
        admin.deleteTable(indexTableName);
      }
      if (admin.tableExists(propTableName)) {
        admin.disableTable(propTableName);
        admin.deleteTable(propTableName);
      }

      //Create user property table
      HTableDescriptor table = new HTableDescriptor(propTableName);
      HColumnDescriptor columnDescriptor = new HColumnDescriptor(FAMILY);
      columnDescriptor.setMaxVersions(1);
      columnDescriptor.setBlocksize(64 * 1024);
      columnDescriptor.setCompressionType(Compression.Algorithm.LZO);
      columnDescriptor.setBloomFilterType(BloomType.ROW);
      columnDescriptor.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
      columnDescriptor.setCacheBloomsOnWrite(true);
      columnDescriptor.setCacheIndexesOnWrite(true);
      table.addFamily(columnDescriptor);
      admin.createTable(table);
      LOG.info("Create " + propTableName + " finish.");

      //Create index table
      table = new HTableDescriptor(indexTableName);
      columnDescriptor = new HColumnDescriptor(FAMILY);
      columnDescriptor.setMaxVersions(1);
      columnDescriptor.setBlocksize(512 * 1024);
      columnDescriptor.setCompressionType(Compression.Algorithm.LZO);
      columnDescriptor.setBloomFilterType(BloomType.ROW);
      columnDescriptor.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
      table.addFamily(columnDescriptor);
      admin.createTable(table);
      LOG.info("Create " + indexTableName + " finish.");

      //Put data
      long bigInt = 0l;
      String currentChar = "a";
      List<Put> puts = new ArrayList<Put>();
      for (long i=0; i<UID_NUM; i++) {
        byte[] five = Arrays.copyOfRange(Bytes.toBytes(i), 3, 8);
        byte[] rk = bytesCombine(date, five);
        for (UserProp up : ups) {
          short id = (short)up.getId();
          byte[] val = null;
          PropType type = up.getPropType();
          switch (type) {
            case sql_bigint:
              val = Bytes.toBytes(bigInt++);
              break;
            case sql_datetime:
              val = Bytes.toBytes(19800101000000l);
              break;
            case sql_string:
              val = Bytes.toBytes(currentChar);
              StringBuilder chars = new StringBuilder(currentChar);
              chars.setCharAt(0, (char)(chars.charAt(0)+1));
              currentChar = chars.toString();
              break;
          }
          Put put = new Put(rk);
          put.add(FAMILY, Bytes.toBytes(id), val);
          puts.add(put);
        }
      }
      HTable propTable = null;
      try {
        propTable = new HTable(conf, propTableName);
        propTable.setAutoFlush(false);
        propTable.put(puts);
        propTable.flushCommits();
      } finally {
        if (propTable != null) {
          propTable.close();
        }
      }
      LOG.info("Construct user property data finish.");
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    //Cleanup meta table
    HTable metaTable = null;
    try {
       metaTable = new HTable(conf, metaTableName);
      List<Delete> deletes = new ArrayList<Delete>();
      byte[] rk = Bytes.toBytes(pID + "_register_time");
      Delete del = new Delete(rk);
      deletes.add(del);

      rk = Bytes.toBytes(pID + "_nation");
      del = new Delete((rk));
      deletes.add(del);

      rk = Bytes.toBytes(pID + "_pay_amount");
      del = new Delete(rk);
      deletes.add(del);

      metaTable.delete(deletes);

      HBaseAdmin admin = new HBaseAdmin(conf);
      admin.majorCompact(metaTableName);

      //Drop test tables
      admin.disableTable(indexTableName);
      admin.deleteTable(indexTableName);

      admin.disableTable(propTableName);
      admin.deleteTable(propTableName);
      LOG.info("Cleanup finish.");

    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (InterruptedException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } finally {
      if (metaTable != null) {
        try {
          metaTable.close();
        } catch (IOException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
      }
    }

  }

  @Test
  public void testReBuild() {
    List<String> pids = new ArrayList<String>();
    pids.add(pID);
    IndexRebuilder ir = new IndexRebuilder(pids, dateStr);
    ir.runRebuildIndex();

    HTable indexTable = null;
    try {
      indexTable = new HTable(conf, indexTableName);
      Scan scan = new Scan();
      scan.setCacheBlocks(false);
      scan.setMaxVersions();
      scan.setBatch(1000);

      short[] ids = {0,1,2};
      for (int i=0; i<ids.length; i++) {
        Set<Long> uidSet = new HashSet<Long>();
        byte[] srk = bytesCombine(Bytes.toBytes(ids[i]), date);
        byte[] erk = bytesCombine(Bytes.toBytes(ids[i]), nextDate);
        scan.setStartRow(srk);
        scan.setStopRow(erk);
        ResultScanner scanner = indexTable.getScanner(scan);
        for (Result r : scanner) {
          KeyValue[] kvs = r.raw();
          for (KeyValue kv : kvs) {
            byte[] uid = kv.getQualifier();
            byte[] three_bytes = new byte[3];
            uidSet.add(Bytes.toLong(bytesCombine(three_bytes, uid)));
          }
        }
        assertEquals(UID_NUM, uidSet.size());
      }

    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } finally {
      if (indexTable != null) {
        try {
          indexTable.close();
        } catch (IOException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
      }
    }

  }

  @Test
  public void testCleanup() {
    List<String> pids = new ArrayList<String>();
    pids.add(pID);
    IndexRebuilder ir = new IndexRebuilder(pids, dateStr);
    ir.runCleanup();

    HTable indexTable = null;
    try {
      indexTable = new HTable(conf, indexTableName);
      Scan scan = new Scan();
      scan.setCacheBlocks(false);
      scan.setMaxVersions();
      scan.setBatch(1000);

      short[] ids = {0,1,2};
      for (int i=0; i<ids.length; i++) {
        Set<Long> uidSet = new HashSet<Long>();
        byte[] srk = bytesCombine(Bytes.toBytes(ids[i]), date);
        byte[] erk = bytesCombine(Bytes.toBytes(ids[i]), nextDate);
        scan.setStartRow(srk);
        scan.setStopRow(erk);
        ResultScanner scanner = indexTable.getScanner(scan);
        for (Result r : scanner) {
          KeyValue[] kvs = r.raw();
          for (KeyValue kv : kvs) {
            byte[] uid = kv.getQualifier();
            byte[] three_bytes = new byte[3];
            uidSet.add(Bytes.toLong(bytesCombine(three_bytes, uid)));
          }
        }
        assertEquals(0, uidSet.size());
      }

    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } finally {
      if (indexTable != null) {
        try {
          indexTable.close();
        } catch (IOException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
      }
    }
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

  public static byte[] getUidOfLong(byte[] uid_five) {
    byte[] uidLong = new byte[8];
    System.arraycopy(uid_five, 0, uidLong, 3, 5);
    return uidLong;
  }

}
