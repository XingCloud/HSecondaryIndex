package com.xingcloud.xa.secondaryindex;

import com.xingcloud.xa.secondaryindex.utils.HTableAdmin;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 5/16/13
 * Time: 9:20 AM
 * To change this template use File | Settings | File Templates.
 */
public class SecondaryIndexWriter {
  
  private static final Log LOG = LogFactory.getLog(SecondaryIndexWriter.class);
  
  public static void main(String args[]){
    LOG.info("Start secondary writer service...");
    HTableAdmin.initHAdmin("single_hbase.xml");
    new Thread(new IndexTailer("/data/log/secondaryindexconfig/")).start();
    //System.out.print(Bytes.toStringBinary(Bytes.toBytes(554062796504L)));
  }
}
