package com.xingcloud.xa.importtool;

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
import java.util.HashMap;
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
    private Map<String, Integer> properties;
    private HTable propertyTable;
    private int maxPropertyID = -1;
    private ExecutorService executor;

    private static Log LOG = LogFactory.getLog(ImportJob.class);

    public ImportJob(Configuration config) throws MasterNotRunningException, ZooKeeperConnectionException {
        this.config = config;
        this.admin = new HBaseAdmin(config);
        this.tables = new ConcurrentHashMap<String, Boolean>();
        this.properties = new HashMap<String, Integer>();
        this.executor = Executors.newFixedThreadPool(32);
    }

    public void batchStart(String baseDir, String[] pids) throws IOException, InterruptedException {
        initTables();
        initProperties();
        long start = System.nanoTime();
        for(String pid: pids){
            start(baseDir, pid);
        }
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
            checkTable(admin, "property_" + pid + "_" + propertyID, "value");
            checkTable(admin, "property_" + pid + "_index", "value");
            ImportWorker worker = new ImportWorker(config, pid, property, propertyID, file);
            executor.execute(worker);
        }
    }

    private void initTables() throws IOException {
        HTableDescriptor[] tableDescriptors = admin.listTables();
        for(HTableDescriptor tableDescriptor: tableDescriptors){
            tables.put(tableDescriptor.getNameAsString(), true);
        }
    }

    private void initProperties() throws IOException {
        Scan scan = new Scan();
        if(!tableExists(admin, "properties"))
            createTable(admin, "properties", "id");
        propertyTable = new HTable(config, "properties");
        ResultScanner scanner = propertyTable.getScanner(scan);
        for(Result row = scanner.next(); row != null; row = scanner.next()){
            String property = Bytes.toString(row.getRow());
            int id = Bytes.toInt(row.getValue(Bytes.toBytes("id"), Bytes.toBytes("id")));
            properties.put(property, id);
            if(id > maxPropertyID) maxPropertyID = id;
        }
        scanner.close();
    }

    private int getPropertyID(String property) throws IOException {
        if(properties.containsKey(property)){
            return properties.get(property);
        } else {
            int id = maxPropertyID + 1;
            Put p = new Put(Bytes.toBytes(property));
            p.add(Bytes.toBytes("id"), Bytes.toBytes("id"), Bytes.toBytes(id));
            propertyTable.put(p);
            maxPropertyID++;
            properties.put(property, id);
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
