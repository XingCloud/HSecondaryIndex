package com.xingcloud.xa.importtool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

/**
 * User: Jian Fang
 * Date: 13-5-28
 * Time: 下午5:02
 */
public class ImportTool {
    public static void main(String[] args) {
        if(args.length <= 1) return;
        Configuration config = HBaseConfiguration.create();
        try {
            String baseDir = args[0];
            String[] pids = new String[args.length - 1];
            for(int i = 1; i < args.length; i++){
                pids[i - 1] = args[i];
            }
            new ImportJob(config).batchStart(baseDir, pids);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
