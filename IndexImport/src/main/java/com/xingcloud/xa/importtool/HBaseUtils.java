package com.xingcloud.xa.importtool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-8-1
 * Time: 下午12:06
 * To change this template use File | Settings | File Templates.
 */
public class HBaseUtils {
  private static Log LOG = LogFactory.getLog(HBaseUtils.class);

  public static void deleteTableFromMETA(String talbe) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    HTable table = new HTable(conf, ".META.");
    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);
    for (Result r : scanner) {
      KeyValue[] kvs = r.raw();
      for (int i=0; i<kvs.length; i++) {
        KeyValue kv = kvs[i];
        byte[] row = kv.getRow();
        String rowStr = Bytes.toString(row);
        if (rowStr.startsWith(talbe + ",")) {
          Delete del = new Delete(row);
          LOG.info("Del row " + rowStr);
          table.delete(del);
          break;
        }
      }
    }
  }

}
