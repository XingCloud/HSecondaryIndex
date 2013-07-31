package com.xingcloud.xa.secondaryindex.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-5-13
 * Time: 下午3:31
 * To change this template use File | Settings | File Templates.
 */
public class WriteUtils {
    private static Log LOG = LogFactory.getLog(WriteUtils.class);

    public static byte[] getFiveBytes(long suid) {
        byte[] rk = new byte[5];
        rk[0] = (byte) (suid>>32 & 0xff);
        rk[1] = (byte) (suid>>24 & 0xff);
        rk[2] = (byte) (suid>>16 & 0xff);
        rk[3] = (byte) (suid>>8 & 0xff);
        rk[4] = (byte) (suid & 0xff);
        return rk;
    }

    public static byte[] getUIIndexRowKey(int propertyID, String date, String attrVal) {
        return bytesCombine(Bytes.toBytes((short)propertyID), Bytes.toBytes(Integer.valueOf(date)),
                Bytes.toBytesBinary(attrVal));
    }

    public static String getUIIndexTableName(String pID) {
        return Constants.PROPERTY_TABLE_PREFIX + pID + Constants.INDEX_TABLE_SUFFIX;
    }
  
    public static byte[] bytesCombine(byte[]... bytesArrays){
        int length = 0;
        for (byte[] bytes: bytesArrays){
            length += bytes.length;
        }
        byte[] combinedBytes = new byte[length];
        int index = 0;
        for (byte[] bytes: bytesArrays){
            for(byte b: bytes){
                combinedBytes[index] = b;
                index++;
            }
        }
        return combinedBytes;
    }
}
