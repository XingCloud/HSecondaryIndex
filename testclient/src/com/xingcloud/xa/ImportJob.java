package com.xingcloud.xa;

import com.xingcloud.xa.uidmapping.UidMappingUtil;
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
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: Jian Fang
 * Date: 13-5-9
 * Time: 下午6:11
 */
public class ImportJob {
    private Configuration config;
    private HBaseAdmin admin;
    private Map<String, Boolean> tables;
    private Map<String, Integer> properties;
    private HTable propertyTable;
    private int maxPropertyID = -1;
    private ObjectMapper objectMapper;
    private ExecutorService executor;
    private long workerStartTS;
    private long workerEndTS;
    private static Log LOG = LogFactory.getLog(ImportJob.class);

    public ImportJob(Configuration config) throws MasterNotRunningException, ZooKeeperConnectionException {
        this.config = config;
        this.admin = new HBaseAdmin(config);
        this.tables = new ConcurrentHashMap<String, Boolean>();
        this.properties = new HashMap<String, Integer>();
        this.objectMapper = new ObjectMapper();
        this.executor = Executors.newFixedThreadPool(8);
        this.workerStartTS = Long.MAX_VALUE;
        this.workerEndTS = 0;
    }

    public void batchStart(String[] files) throws IOException, InterruptedException {
        initTables();
        initProperties();
        long start = System.currentTimeMillis();
        for (String file: files){
            start(file);
        }
        executor.shutdown();
        while(!executor.isTerminated()){
            Thread.sleep(100);
        }
        long end = System.currentTimeMillis();
        LOG.info("duration: " + (end - start) + "ms, thread time: " + (workerEndTS - workerStartTS) + "ms");
    }

    public void start(String filePath) throws IOException, InterruptedException {
        InputStreamReader inputStream = new InputStreamReader(new FileInputStream(new File(filePath)));
        BufferedReader reader = new BufferedReader(inputStream);
        String line = reader.readLine();
        int count = 0;
        Map<String, ImportWorkerInfo> workerInfos = new HashMap<String, ImportWorkerInfo>();
        long start = System.currentTimeMillis();
        while(line != null){
            String[] words = line.split("\t");
            processLine(workerInfos, words[0], UidMappingUtil.getInstance().decorateWithMD5(Long.parseLong(words[1])), words[2]);
            line = reader.readLine();
            count++;
            if(count % 10000 == 0 || line == null){
                for(String tableName: workerInfos.keySet()){
                    ImportWorker worker = new ImportWorker(workerInfos.get(tableName), this);
                    executor.execute(worker);
                }
                workerInfos.clear();
                LOG.info("finished #" + count + ", use " + (System.currentTimeMillis() - start) + "ms");
                start = System.currentTimeMillis();
            }
        }
        reader.close();
        inputStream.close();

    }

    public void checkTable(HBaseAdmin admin, String tableName, String... families) throws IOException {
        if(!tableExists(admin, tableName)){
            createTable(admin, tableName, families);
        }
    }

    public Configuration getConfig(){
        return config;
    }

    synchronized public void addDuration(long startTS, long endTS) {
        if(workerStartTS > startTS)
            workerStartTS = startTS;
        if(workerEndTS < endTS)
            workerEndTS = endTS;
    }

    private void processLine(Map<String, ImportWorkerInfo> workerInfos, String pid, long uid, String data) throws IOException {
        Map<String, Object> parsedData = objectMapper.readValue(data, Map.class);
        for(String property: parsedData.keySet()){
            int propertyID = getPropertyID(property);
            checkTable(admin, "property_" + pid + "_" + propertyID, "value");
            checkTable(admin, "property_" + pid + "_index", "value");
            ImportWorkerInfo info = workerInfos.get(pid + "_" + propertyID);
            if(info == null){
                info = new ImportWorkerInfo(config, pid, property, propertyID);
                workerInfos.put(pid + "_" + propertyID, info);
            }
            info.getData().put(uid, parsedData.get(property));
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

    private void initTables() throws IOException {
        HTableDescriptor[] tableDescriptors = admin.listTables();
        for(HTableDescriptor tableDescriptor: tableDescriptors){
            tables.put(tableDescriptor.getNameAsString(), true);
        }
    }
}
