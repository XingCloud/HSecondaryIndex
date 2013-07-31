package com.xingcloud.xa.secondaryindex;

import com.xingcloud.xa.secondaryindex.utils.Constants;
import com.xingcloud.xa.secondaryindex.utils.HTableAdmin;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;

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
    LOG.info("Start secondaryindex writer service...");

    HTableAdmin.initHAdmin("single_hbase.xml");
    String configDir = Constants.SECONDARY_INDEX_LOG_DIR + File.separator + "config";
    new Thread(new IndexTailer(configDir)).start();
  }
}
