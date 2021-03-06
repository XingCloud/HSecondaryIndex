package com.xingcloud.xa.secondaryindex.manager;

import com.xingcloud.xa.secondaryindex.utils.HTableAdmin;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-7-11
 * Time: 下午6:52
 * To change this template use File | Settings | File Templates.
 */
public class HBaseResourceManager {
    private static Log logger = LogFactory.getLog(HBaseResourceManager.class);
    private HTablePool pool;
    private final int MAX_SIZE = 32;
    private static HBaseResourceManager instance;

    public synchronized static HBaseResourceManager getInstance() throws IOException {
        if (instance == null) {
            instance = new HBaseResourceManager();
        }
        return instance;
    }

    private HBaseResourceManager() throws IOException {
        this.pool = new HTablePool(HTableAdmin.getHBaseConf(), MAX_SIZE);
    }

    public HTableInterface getTable(byte[] tableName) throws IOException {
        return pool.getTable(tableName);
    }

    public HTableInterface getTable(String tableName) throws IOException {
        HTableInterface htable = null;
        try {
            htable = pool.getTable(tableName);
        } catch (Exception e) {
            logger.error("Get htable got exception! MSG: " + e.getMessage());
            e.printStackTrace();
            throw new IOException("HTable pool get table got exception! " + tableName);
        }
        return htable;
    }

    public void putTable(HTableInterface htable) throws IOException {
        if (htable != null) {
            htable.close();
        }
    }

    public void closeAll() throws IOException {
        this.pool.close();
    }

    public void closeAll(String projectId) throws IOException {
        this.pool.closeTablePool(projectId + "_deu");
    }

    public void closeAllConnections() {
        HConnectionManager.deleteAllConnections();
    }
}
