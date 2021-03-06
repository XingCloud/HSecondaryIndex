package com.xingcloud.xa.importtool;

import com.xingcloud.mysql.MySql_fixseqid;
import com.xingcloud.userprops_meta_util.PropType;
import com.xingcloud.userprops_meta_util.UserProp;
import com.xingcloud.userprops_meta_util.UserProps_DEU_Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: Jian Fang
 * Date: 13-5-28
 * Time: 下午5:02
 */
public class ImportJob {
    private Configuration config;
    private HBaseAdmin admin;
    private Map<String, Boolean> tables;
    private Map<String, UserProp> propertiesMeta; // store a project's properties meta
    private ExecutorService executor;

    private static final String  CF = "val";
    private static final String PROPERTY_TABLE_PREFIX = "properties_";
    private static final String INDEX_TALBE_SUFFIX = "_index";
    private static final int POOL_SIZE = 8;

    private static Log LOG = LogFactory.getLog(ImportJob.class);

    public ImportJob(Configuration config) throws IOException {
        this.config = config;
        this.admin = new HBaseAdmin(config);
        this.tables = new ConcurrentHashMap<String, Boolean>();
        this.executor = Executors.newFixedThreadPool(POOL_SIZE);
    }

    public void batchStart(String baseDir, String[] pids) throws IOException, InterruptedException {
        initTables();
        checkTable(admin, "meta_properties", CF);

        long start = System.nanoTime();
        for(String pid: pids){
            this.propertiesMeta = new HashMap<String, UserProp>(); // init for next project
            importPropertiesMeta(pid);
            checkTable(admin, "properties_" + pid, CF);
            importProperties(baseDir, pid);
        }

        executor.shutdown();
        while(!executor.isTerminated()){
          Thread.sleep(100);
        }

        long end = System.nanoTime();
        LOG.info("all done! duration: " + ((end - start) / 1000000) + "ms");
    }

  public void importProperties(String baseDir, String pid) throws IOException {
    File folder = new File(baseDir + "/" + pid);
    File[] fileList = folder.listFiles();
    if (fileList == null){
        LOG.error("No files found in directory: " + folder);
        return;
    }

    for(File file: fileList){
        String name = file.getName();
        int start = name.indexOf("_") + 1;
        int end = name.indexOf(".log");

        String propertyName = name.substring(start, end);
        UserProp userProp = propertiesMeta.get(propertyName);

        ImportWorker worker = new ImportWorker(config, pid, userProp, file);
        executor.execute(worker);
    }
  }

  private void initTables() throws IOException {
      HTableDescriptor[] tableDescriptors = admin.listTables();
      for(HTableDescriptor tableDescriptor: tableDescriptors){
          tables.put(tableDescriptor.getNameAsString(), true);
      }
  }

  private void importPropertiesMeta(String pid) throws IOException {
    Connection conn = null;
    ResultSet rs = null;
    Statement statement = null;
    List<Put> puts = new ArrayList<Put>();

    try{
      conn = MySql_fixseqid.getInstance().getConnLocalNode(pid);
      statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.setFetchSize(Integer.MIN_VALUE);
      rs = statement.executeQuery("select * from sys_meta");
      int i=0;
      while (rs.next()){
        String name = rs.getString("prop_name");
        String type = rs.getString("prop_type");
        String func = rs.getString("prop_func");
        String orig = rs.getString("prop_orig");
        
        Put put = new Put(Bytes.toBytes(pid+"_"+name));
        put.add(Bytes.toBytes(CF), Bytes.toBytes("id"), Bytes.toBytes(i));
        put.add(Bytes.toBytes(CF), Bytes.toBytes("type"), Bytes.toBytes(type));
        put.add(Bytes.toBytes(CF), Bytes.toBytes("func"), Bytes.toBytes(func));
        put.add(Bytes.toBytes(CF), Bytes.toBytes("orig"), Bytes.toBytes(orig));

        puts.add(put);
        i++;
      }
    }catch (SQLException e){
      e.printStackTrace();
      LOG.error(e.getMessage());  
    } finally {
	  if(rs != null){
		  try{
			  rs.close();
		  }catch (SQLException ex){
			  ex.printStackTrace();
		  }
	  }
	  if(statement != null){
		  try{
			  statement.close();
		  }catch (SQLException ex){
			  ex.printStackTrace();
		  }
	  }
	  if(conn != null){
		  try{
			  conn.close();
		  }catch (SQLException ex){
			  ex.printStackTrace();
		  }
	  }
    }

    LOG.info("properties size:"+puts.size());
    HTable table = null;
    try {
      table = new HTable(config, "meta_properties");
      table.put(puts);
    } catch (IOException e) {
      LOG.error(pid + ":import properties error.");
      e.printStackTrace();  
    }finally {
      if(table != null){
        try {
          table.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    List<UserProp> props = UserProps_DEU_Util.getInstance().getUserProps(pid);
    LOG.info("Project: " + pid + ", Properties:");
    for (UserProp up : props) {
      LOG.info(up.getPropName());
      propertiesMeta.put(up.getPropName(), up);
    }
  }

  private boolean tableExists(String tableName) throws IOException {
      return tables.containsKey(tableName);
  }

  private void createTable(HBaseAdmin admin, String tableName, String... families) throws IOException {
    LOG.info("create table: " + tableName);
    HTableDescriptor table = new HTableDescriptor(tableName);
    for(String family: families){
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
        columnDescriptor.setMaxVersions(2000);
        columnDescriptor.setCompressionType(Compression.Algorithm.LZO);
        columnDescriptor.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
        columnDescriptor.setBloomFilterType(BloomType.ROW);
        table.addFamily(columnDescriptor);
    }
    admin.createTable(table);
    tables.put(tableName, true);
  }

  public void checkTable(HBaseAdmin admin, String tableName, String... families) throws IOException {
    if(!tableExists(tableName)){
        createTable(admin, tableName, families);
    }
  }
  
  public void batchRemove(String[] pids){
    try{
        initTables();
        for(String pid: pids){
            String propertyTableName = PROPERTY_TABLE_PREFIX + pid;
            String indexTableName = propertyTableName + INDEX_TALBE_SUFFIX;

            if(tableExists(propertyTableName)){
                LOG.info("disable and delete table: " + propertyTableName);
                admin.disableTable(propertyTableName);
                admin.deleteTable(propertyTableName);
            }

            if(tableExists(indexTableName)){
                LOG.info("disable and delete table: " + indexTableName);
                admin.disableTable(indexTableName);
                admin.deleteTable(indexTableName);
            }
        }
    }catch (IOException ex){
        ex.printStackTrace();
    }
  }
}
