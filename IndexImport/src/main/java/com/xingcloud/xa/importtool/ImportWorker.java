package com.xingcloud.xa.importtool;

import com.xingcloud.userprops_meta_util.PropType;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * User: Jian Fang
 * Date: 13-5-28
 * Time: 下午5:02
 */
public class ImportWorker implements Runnable  {
    private Configuration config;
    private String pid;
    private String property;
    private int propertyID;
    private PropType propertyType;
    private File logFile;

    private static Log LOG = LogFactory.getLog(ImportWorker.class);

    public ImportWorker(Configuration config, String pid, String property, int propertyID, PropType propertyType, File logFile){
      this.config = config;
      this.pid = pid;
      this.property = property;
      this.propertyID = propertyID;
      this.propertyType = propertyType;
      this.logFile = logFile;
    }

    @Override
    public void run() {
        long start = System.nanoTime();
        long count = 0;
        try {
            HTable table = new HTable(config, "properties_" + pid);
            List<Put> puts = new ArrayList<Put>();
            InputStreamReader inputStream = new InputStreamReader(new FileInputStream(logFile));
            BufferedReader reader = new BufferedReader(inputStream);
            String line = reader.readLine();
            while(line != null){
                String[] words = line.split("\t");
                if(words.length != 2){
                    line = reader.readLine();
                    continue;
                }
                long uid = Long.parseLong(words[0]);
                String value = words[1];
                byte[] uidBytes = Bytes.toBytes(uid);
                byte[] shortenUid = {uidBytes[3],uidBytes[4], uidBytes[5], uidBytes[6], uidBytes[7]};
                Put dataPut = new Put(shortenUid);
                if(propertyType.equals("sql_datetime") || propertyType.equals("sql_bigint")){
                    dataPut.add(Bytes.toBytes("val"), Bytes.toBytes(propertyID), Bytes.toBytes(Long.parseLong(value)));
                } else{
                    dataPut.add(Bytes.toBytes("val"), Bytes.toBytes(propertyID), Bytes.toBytes(value));
                }
                puts.add(dataPut);
                line = reader.readLine();
                if(puts.size() == 10000 || line == null){
                    table.put(puts);
                    puts.clear();
                }
                count++;
            }
            reader.close();
            inputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            long end = System.nanoTime();
            long duration = (end - start) / 1000000;
            LOG.info("finish import " + pid + "'s " + property + ", count: " + count + ", use: " + duration + "ms");
        }
    }
}
