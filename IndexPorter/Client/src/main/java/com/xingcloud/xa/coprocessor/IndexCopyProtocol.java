package com.xingcloud.xa.coprocessor;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 5/28/13
 * Time: 2:36 PM
 * To change this template use File | Settings | File Templates.
 */
public interface IndexCopyProtocol  extends CoprocessorProtocol{
     public int copyIndex(byte[] startKey,byte[] stopKey ,String tableName) ;
}
