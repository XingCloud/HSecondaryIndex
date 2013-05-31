package com.xingcloud.xa;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 5/27/13
 * Time: 2:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class DailyIndexCopy {
    public static Log LOG = LogFactory.getLog(DailyIndexCopy.class);

    public static void main(String args[]){
        try {
            Configuration config = HBaseConfiguration.create();
            CopyJob dailyIndexCopy = new CopyJob(config) ;
            dailyIndexCopy.exec();
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
