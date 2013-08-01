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
        if(args.length <= 1) {
            System.err.println("Incorrect args.");
            System.out.println("Usage:\n" +
                    "\tjava -jar IndexImport-1.0-jar-with-dependencies.jar remove projectID1 projectID2 ...\n" +
                    "\tjava -jar IndexImport-1.0-jar-with-dependencies.jar dataDirImportFrom  projectID1 projectID2 ." +
                    "..");
            return;
        }

        Configuration config = HBaseConfiguration.create();
        try {
            String[] pids = new String[args.length - 1];
            System.arraycopy(args, 1, pids, 0, args.length - 1);

            if(args[0].equals("remove")){
                new ImportJob(config).batchRemove(pids);
            } else if (args[0].equals("del_from_meta")) {
                HBaseUtils.deleteTableFromMETA(args[1]);
            } else{
            new ImportJob(config).batchStart(args[0], pids);
          }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
