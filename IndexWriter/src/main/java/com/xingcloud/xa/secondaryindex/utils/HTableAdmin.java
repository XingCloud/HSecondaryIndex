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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 5/28/13
 * Time: 8:02 AM
 * To change this template use File | Settings | File Templates.
 */
public class HTableAdmin {
  private static Log LOG = LogFactory.getLog(HTableAdmin.class);
  private static Map<String, HBaseAdmin> admins = new HashMap<String, HBaseAdmin>();
  private static Map<String, Configuration> hbaseConfs = new HashMap<String, Configuration>();
  private static Map<String, Map<String, Boolean>> tables = new HashMap<String, Map<String, Boolean>>();

  public static Configuration getHBaseConf(String host) {
    return hbaseConfs.get(host);
  }

  public static Map<String, HBaseAdmin> getAdmins() {
    return admins;
  }

  public static Map<String, Map<String, Boolean>> getTables() {
    return tables;
  }
 
  public static void initHAdmin(String file){
    LOG.info("load hbase infomation");
    Dom dom = ConfigReader.getDom(file);
    List<Dom> hbaseDomList = dom.elements("hbase");
    for (Dom hbase : hbaseDomList) {
      String host = hbase.elementText("zookeeper");
      try {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", host);
        conf.set("hbase.zookeeper.property.clientPort", Constants.HBASE_PORT);
        HBaseAdmin admin = new HBaseAdmin(conf);  
        hbaseConfs.put(host, conf);
        admins.put(host, admin);
        initTables(host);
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error("Exception when init proxy", e);
      }
    } 
    LOG.info("finish load hbase information");
  }
  
  private static void initTables(String host) {
    try {
      if (!tables.containsKey(host)){
        tables.put(host, new HashMap<String, Boolean>());
      }
      
      HTableDescriptor[] tableDescriptors = admins.get(host).listTables();
      for(HTableDescriptor tableDescriptor: tableDescriptors){
        
        tables.get(host).put(tableDescriptor.getNameAsString(), true);
      }
    }catch (Exception e){
      e.printStackTrace();
    }

  }

  private static void createTable(String host, String tableName, String... families) throws IOException {
    HBaseAdmin admin = admins.get(host);
    HTableDescriptor table = new HTableDescriptor(tableName);
    for(String family: families){
      HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
      if(tableName.endsWith("_index"))
        columnDescriptor.setMaxVersions(1);
      columnDescriptor.setBlocksize(512 * 1024);
      columnDescriptor.setCompressionType(Compression.Algorithm.LZO);
      table.addFamily(columnDescriptor);
    }
    admin.createTable(table);
    tables.get(host).put(tableName, true);
  }

  public static void checkTable(String host, String tableName, String... families) throws IOException {
    if(!tableExists(host, tableName)){
      createTable(host, tableName, families);
    }
  }

  private static  boolean tableExists(String host, String tableName) throws IOException {   
    return tables.containsKey(host) && tables.get(host).containsKey(tableName);
  }

}
