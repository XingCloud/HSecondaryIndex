package com.xingcloud.xa.importtool;

import com.xingcloud.mysql.MySql_fixseqid;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
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
    private Map<String, Map<String, String>> properties;
    private HTable propertyTable;
    private int maxPropertyID = -1;
    private ExecutorService executor;

    private static Log LOG = LogFactory.getLog(ImportJob.class);

    public ImportJob(Configuration config) throws MasterNotRunningException, ZooKeeperConnectionException {
        this.config = config;
        this.admin = new HBaseAdmin(config);
        this.tables = new ConcurrentHashMap<String, Boolean>();
        this.executor = Executors.newFixedThreadPool(32);
    }

    public void batchStart(String baseDir, String[] pids) throws IOException, InterruptedException {
        initTables();
        long start = System.nanoTime();
        for(String pid: pids){
          this.properties = new HashMap<String, Map<String, String>>(); // init for next project
          initProperties(pid);
          start(baseDir, pid);
        }
        executor.shutdown();
        while(!executor.isTerminated()){
            Thread.sleep(100);
        }
        long end = System.nanoTime();
        LOG.info("all done! duration: " + ((end - start) / 1000000) + "ms");
    }

    public void start(String baseDir, String pid) throws IOException {
        File folder = new File(baseDir + "/" + pid);
        for(File file: folder.listFiles()){
            String name = file.getName();
            int start = name.indexOf("_") + 1;
            int end = name.indexOf(".log");
            String property = name.substring(start, end);
            int propertyID = getPropertyID(property);
            String propertyType = properties.get(property).get("type");
            checkTable(admin, "property_" + pid, "value");
            ImportWorker worker = new ImportWorker(config, pid, property, propertyID, propertyType, file);
            executor.execute(worker);
        }
    }

    private void initTables() throws IOException {
        HTableDescriptor[] tableDescriptors = admin.listTables();
        for(HTableDescriptor tableDescriptor: tableDescriptors){
            tables.put(tableDescriptor.getNameAsString(), true);
        }
    }

    private void initProperties(String pid) throws IOException {
        Scan scan = new Scan();
        if(!tableExists(admin, "properties_"+pid)){
            createTable(admin, "properties_"+pid, "value");
            importProperties(pid);
        }
        propertyTable = new HTable(config, "properties");
        ResultScanner scanner = propertyTable.getScanner(scan);
        for(Result row = scanner.next(); row != null; row = scanner.next()){
            String property = Bytes.toString(row.getRow());
            Map<String, String> meta = new HashMap<String, String>();
            int id = Bytes.toInt(row.getValue(Bytes.toBytes("value"), Bytes.toBytes("id")));
            String type = Bytes.toString(row.getValue(Bytes.toBytes("value"), Bytes.toBytes("type")));
            meta.put("id", String.valueOf(id));
            meta.put("type", type);
            properties.put(property, meta);
            if(id > maxPropertyID) maxPropertyID = id;
        }
        scanner.close();
    }

  private void importProperties(String pid) {
    Connection conn;
    ResultSet rs;
    List<Put> puts = new ArrayList<Put>();
    try{
       conn = MySql_fixseqid.getInstance().getConnLocalNode("fix_"+pid);
       Statement statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
       statement.setFetchSize(Integer.MIN_VALUE);
       rs = statement.executeQuery("select * from sys_meta");
      int i=0;
      while (rs.next()){
        String name = rs.getString("prop_name");
        String type = rs.getString("prop_type");
        String func = rs.getString("prop_func");
        String orig = rs.getString("prop_orig");
        
        Put put = new Put(Bytes.toBytes(name));
        put.add(Bytes.toBytes("value"), Bytes.toBytes("id"), Bytes.toBytes(i));
        put.add(Bytes.toBytes("value"), Bytes.toBytes("type"), Bytes.toBytes(type));
        put.add(Bytes.toBytes("value"), Bytes.toBytes("func"), Bytes.toBytes(func));
        put.add(Bytes.toBytes("value"), Bytes.toBytes("orig"), Bytes.toBytes(orig));

        puts.add(put);
        i++;
      }
    }catch (Exception e){
      
    }

    HTable table = null;
    try {
      table = new HTable(config, "property_" + pid);
      table.put(puts);
      table.flushCommits();
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }finally {
      try {
        table.close();
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
  }

  private int getPropertyID(String property) throws IOException {
        if(properties.containsKey(property)){
            return Integer.valueOf(properties.get(property).get("id"));
        } else {
            int id = maxPropertyID + 1;
            Put p = new Put(Bytes.toBytes(property));
            p.add(Bytes.toBytes("value"), Bytes.toBytes("id"), Bytes.toBytes(id));
            propertyTable.put(p);
            maxPropertyID++;
            Map<String, String> meta = new HashMap<String, String>();
            meta.put("id", String.valueOf(id));
            properties.put(property, meta);
            return id;
        }
    }

    private boolean tableExists(HBaseAdmin admin, String tableName) throws IOException {
        return tables.containsKey(tableName);
    }

    private void createTable(HBaseAdmin admin, String tableName, String... families) throws IOException {
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
        tables.put(tableName, true);
    }

    public void checkTable(HBaseAdmin admin, String tableName, String... families) throws IOException {
        if(!tableExists(admin, tableName)){
            createTable(admin, tableName, families);
        }
    }
}
