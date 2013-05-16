package com.xingcloud.xa;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Jian Fang
 * Date: 13-5-9
 * Time: 下午6:34
 */
public class ImportWorkerInfo {
    private Configuration config;
    private String tableName;
    private String property;
    private int propertyID;

    public String getTableName() {
        return tableName;
    }

    public String getProperty() {
        return property;
    }

    public int getPropertyID() {
        return propertyID;
    }

    public Map<Long, Object> getData() {
        return data;
    }

    public Configuration getConfig(){
        return config;
    }

    private Map<Long, Object> data;

    public ImportWorkerInfo(Configuration config, String tableName,
                        String property, int propertyID){
        this.config = config;
        this.tableName = tableName;
        this.property = property;
        this.propertyID = propertyID;
        this.data = new HashMap<Long, Object>();
    }
}
