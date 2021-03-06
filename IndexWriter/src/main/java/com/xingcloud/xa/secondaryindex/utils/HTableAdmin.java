package com.xingcloud.xa.secondaryindex.utils;

import com.xingcloud.xa.secondaryindex.utils.config.ConfigReader;
import com.xingcloud.xa.secondaryindex.utils.config.Dom;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 5/28/13
 * Time: 8:02 AM
 * To change this template use File | Settings | File Templates.
 */
public class HTableAdmin {
  private static Log LOG = LogFactory.getLog(HTableAdmin.class);
  private static HBaseAdmin admin;
  private static Configuration hbaseConf = new Configuration();
  private static Map<String, Boolean> tables = new ConcurrentHashMap<String, java.lang.Boolean>();

  public static Configuration getHBaseConf() {
    return hbaseConf;
  }

  public static HBaseAdmin getAdmin() {
    return admin;
  }

  public static Map<String, Boolean> getTables() {
    return tables;
  }
 
  public static void initHAdmin(String file){
    LOG.info("load hbase infomation");
    Dom dom = ConfigReader.getDom(file);
    Dom hbase = dom.element("hbase");
    String host = hbase.elementText("zookeeper");
    hbaseConf = HBaseConfiguration.create();
    hbaseConf.set("hbase.zookeeper.quorum", host);
    hbaseConf.set("hbase.zookeeper.property.clientPort", Constants.HBASE_PORT);

    try {
      admin = new HBaseAdmin(hbaseConf);
      initTables();
    } catch (IOException e) {
      e.printStackTrace();
      LOG.error("Exception when init proxy", e);
    }
    LOG.info("finish load hbase information");
  }
  
  private static void initTables() {
    try {
      HTableDescriptor[] tableDescriptors = admin.listTables();
      for(HTableDescriptor tableDescriptor: tableDescriptors){
        String tableName = tableDescriptor.getNameAsString();
        LOG.info("Add " + tableName + " to table list...");
        tables.put(tableName, true);
      }
      LOG.info("Init tables finished.");
    }catch (Exception e){
      e.printStackTrace();
    }
  }

  private static void createTable(String tableName, String... families) throws IOException {
    // this method is only for creating index table
    LOG.info("Begin to create index table " + tableName);

    HTableDescriptor table = new HTableDescriptor(tableName);
    for(String family: families){
      HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
      if(tableName.endsWith("_index"))
        columnDescriptor.setMaxVersions(1);
        columnDescriptor.setBlocksize(512 * 1024);
        columnDescriptor.setCompressionType(Compression.Algorithm.LZO);
        columnDescriptor.setBloomFilterType(BloomType.ROW);
        columnDescriptor.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
        table.addFamily(columnDescriptor);
    }

    admin.createTable(table);
    LOG.info("Create table " + tableName + " successfully!");

    tables.put(tableName, true);
    LOG.info("Add table " + tableName + " to table list. Size: " + tables.size());
  }

  public static void checkTable(String tableName, String... families) throws IOException {
    LOG.info("Begin to check table " + tableName);
    if(!tableExists(tableName)){
      LOG.info("Table list doesn't contain " + tableName + " Size: " + tables.size());
      createTable(tableName, families);
    }
  }

  private static boolean tableExists(String tableName) throws IOException {
    return tables.containsKey(tableName);
  }

}
