package com.xingcloud.xa.importtool;

import com.xingcloud.userprops_meta_util.PropType;
import com.xingcloud.userprops_meta_util.UpdateFunc;
import com.xingcloud.userprops_meta_util.UserProp;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
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
    private UserProp userProp;
    private File logFile;

    private static final int PUT_SIZE = 10000;

    private static Log LOG = LogFactory.getLog(ImportWorker.class);

    public ImportWorker(Configuration config, String pid, UserProp userProp, File logFile){
        this.config = config;
        this.pid = pid;
        this.userProp = userProp;
        this.logFile = logFile;
    }

    @Override
    public void run() {
        long start = System.nanoTime();
        long count = 0;
        try {
            HTable table = new HTable(config, "properties_" + pid);
            List<Row> puts = new ArrayList<Row>();
            InputStreamReader inputStream = new InputStreamReader(new FileInputStream(logFile));
            BufferedReader reader = new BufferedReader(inputStream);

            LOG.info("Importing property: " + userProp.getPropName());

            UpdateFunc updateFunc = userProp.getPropFunc();
            long timestamp = TimeUtil.startTimestampOfToday();
            if(updateFunc == UpdateFunc.once){
                timestamp = Long.MAX_VALUE - timestamp;
            }

            String line = reader.readLine();
            while(line != null){
                String[] words = line.split("\t");
                if(words.length != 2){
                    LOG.warn("Invalid line: " + line);
                    line = reader.readLine();
                    continue;
                }

                long uid = Long.parseLong(words[0]);
                String value = words[1];
                byte[] uidBytes = Bytes.toBytes(uid);
                byte[] shortenUid = {uidBytes[3],uidBytes[4], uidBytes[5], uidBytes[6], uidBytes[7]};

                Put dataPut = new Put(shortenUid);
                dataPut.setDurability(Durability.SKIP_WAL);

                PropType propertyType = userProp.getPropType();
                int propertyID = userProp.getId();

                if(propertyType == PropType.sql_datetime || propertyType == PropType.sql_bigint) {
                    dataPut.add(Bytes.toBytes("val"), Bytes.toBytes(propertyID), timestamp,
                            Bytes.toBytes(Long.parseLong(value)));
                } else{
                    dataPut.add(Bytes.toBytes("val"), Bytes.toBytes(propertyID), timestamp,
                            Bytes.toBytes(value));
                }

                puts.add(dataPut);
                line = reader.readLine();
                if(puts.size() == PUT_SIZE || line == null){
                    long batchStartTime = System.currentTimeMillis();

                    table.batch(puts);

                    long batchEndTime = System.currentTimeMillis();
                    LOG.info("Puts size: " + puts.size() + " Time cost: " +
                            (batchEndTime - batchStartTime) / 1000.0 + " seconds");

                    puts.clear();
                }
                count++;
            }

            reader.close();
            inputStream.close();
            table.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            long end = System.nanoTime();
            long duration = (end - start) / 1000000;
            LOG.info("finish import " + pid + "'s " + userProp.getPropName() + ", count: " + count + ", " +
                    "use: " + duration + "ms");
        }
    }
}
