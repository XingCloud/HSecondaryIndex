package com.xingcloud.xa;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 5/27/13
 * Time: 2:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class CopyJob {
    private HBaseAdmin admin;
    private Set<String> tables;
    private Map<String,Integer> properties ;
    private Configuration config;
    private ExecutorService executor ;

    private static Log LOG = LogFactory.getLog(CopyJob.class);

    public CopyJob(Configuration config) throws MasterNotRunningException, ZooKeeperConnectionException {
        this.config = config;
        this.admin = new HBaseAdmin(config);
        this.tables = new HashSet<String>();
        properties = new HashMap<String,Integer>() ;
        this.executor = Executors.newFixedThreadPool(8);
    }

    public void exec() throws IOException, InterruptedException {
        LOG.info("Daily Index Copy start .");
        long start = System.currentTimeMillis() ;
        initProperties();
        initTables();
        for(String tableName : tables){
            CopyWorker copyWorker = new CopyWorker(admin,tableName,properties,config) ;
            executor.execute(copyWorker);
        }
        executor.shutdown();
        while(!executor.isTerminated()){
            Thread.sleep(100);
        }
        long timeCost = System.currentTimeMillis() - start ;
        LOG.info("Daily Index Copy finished , cost " + timeCost/1000 + " seconds .");
    }


    private void initTables() throws IOException {
        LOG.info("List index tables.");
        HTableDescriptor[] tableDescriptors = admin.listTables();
        for (HTableDescriptor hTableDescriptor : tableDescriptors) {
            String tableName = hTableDescriptor.getNameAsString();
            if (tableName.endsWith("_index")){
                tables.add(tableName);
                LOG.info("Add index table : " + tableName);
            }
        }
        LOG.info("Index tables size : " + tables.size());
    }

    private void initProperties() throws IOException{
        LOG.info("List properties");
        Scan scan = new Scan() ;
        scan.setMaxVersions(1);
        HTable propertyTable = new HTable(config,"properties") ;
        ResultScanner scanner = propertyTable.getScanner(scan);
        for(Result row = scanner.next() ; row != null ; row = scanner.next()){
            String property = Bytes.toString(row.getRow());
            int propertyID = Bytes.toInt(row.getValue(Bytes.toBytes("id"),Bytes.toBytes("id")));
            properties.put(property,propertyID);
            LOG.info("property map : " + property + "=>" + propertyID);
        }
        LOG.info("Property table size : " + properties.size());
        scanner.close();
        propertyTable.close();
    }


}
